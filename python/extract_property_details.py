from datetime import datetime

import httpx
from bs4 import BeautifulSoup
from sayn import task
from sayn.database import Database
from sayn.tasks.task import Task

import pandas as pd

from .utils import format_field_name

BASE_URL = "https://www.rightmove.co.uk/"

template = {
    "property_id": "",
    "property_url": "",
    "location_name": "",
    "rent_pcm": "",
    "let_available_date": "",
    "deposit": "",
    "min_tenancy": "",
    "let_type": "",
    "furnish_type": "",
    "property_type": "",
    "bedrooms": "",
    "bathrooms": "",
    "size": "",
    "epc_rating_url": "",
    "title": "",
    "image": "",
    "description": "",
    "date_added": datetime.today(),
}


@task(sources="raw.property_links", outputs="intermediate.property_details")
def extract_property_details(context: Task, warehouse: Database):
    src_table = context.src("raw.property_links")
    properties = warehouse.read_data(
        f"""
    SELECT *
      FROM (
        SELECT *
             , ROW_NUMBER() OVER (PARTITION BY property_id ORDER BY date_added DESC) AS n
          FROM {src_table}
        )
    WHERE n = 1
    """
    )

    # print(properties)

    p_buckets = int(len(properties) / 50)
    n_properties = len(properties)
    context.set_run_steps(
        [f"Get Property {p * 50} / {n_properties}" for p in range(0, p_buckets + 1)]
    )

    properties_details = []
    with httpx.Client(base_url=BASE_URL) as client:
        for idx, _property in enumerate(properties):
            if (stp := idx % 50) == 0:
                context.start_step(f"Get Property {idx} / {n_properties}")

            property_url = _property["property_url"]
            property_info = template.copy()
            property_info["property_id"] = _property["property_id"]
            property_info["property_url"] = BASE_URL[:-1] + property_url
            property_info["location_name"] = _property["location_name"]
            property_info["image"] = _property["image"]
            property_info["title"] = _property["title"]
            property_info["description"] = _property["description"]

            try:
                property_response = client.get(property_url)
            except Exception as e:
                context.info(e)
                continue

            property_soup = BeautifulSoup(property_response.text, "html.parser")

            if rent := property_soup.select_one("div._1gfnqJ3Vtd1z40MlC0MzXu span"):
                rent_pcm = (
                    rent.text.replace("£", "")
                    .replace("pcm", "")
                    .replace(",", "")
                    .strip()
                )
                property_info["rent_pcm"] = int(rent_pcm)

            if property_detail := property_soup.select(
                "dl._2E1qBJkWUYMJYHfYJzUb_r div"
            ):
                for dl in property_detail:
                    property_info[format_field_name(dl.dt.string)] = str(
                        dl.dd.string
                        or dl.dd.span.next_sibling
                        or dl.dd.span.previous_sibling
                    )
            if property_type := property_soup.select("div._4hBezflLdgDMdFtURKTWh dl"):
                for dl in property_type:
                    property_info[format_field_name(dl.dt.string)] = str(dl.dd.string)

            epc_rating = property_soup.select_one(
                "div._3BAkOrQAfGZMsQDtC0WdbO._3A8p_O-xNhCM7MwsZ_g0yj a"
            )
            property_info["epc_rating_url"] = epc_rating["href"] if epc_rating else ""

            property_info["deposit"] = property_info["deposit"].replace(",", "")
            property_info["bedrooms"] = (
                property_info["bedrooms"][-1] if property_info["bedrooms"] else ""
            )
            property_info["bathrooms"] = (
                property_info["bathrooms"][-1] if property_info["bathrooms"] else ""
            )

            if property_info["size"]:
                property_info["size"] = property_info["size"].split(" ")[0]
                property_info["size"] = int(property_info["size"].replace(",", "").replace("-",""))
                property_info["size"] = property_info["size"] * 0.092903
            else:
                property_info["size"] = ""
            property_info = {k: property_info[k] for k in template.keys()}
            if not property_soup.select(
                "span.ksc_lozenge.berry._2WqVSGdiq2H4orAZsyHHgz"
            ):
                if property_info["let_available_date"] in ("Now", "Ask agent"):
                    properties_details.append(property_info)
                else:
                    try:
                        if int(property_info["let_available_date"].split("/")[1]) in (6, 7):
                            properties_details.append(property_info)
                    except:
                        context.info(f'Found something weird {property_info["let_available_date"]}')

            if stp == 49:
                context.finish_current_step()

    warehouse.load_data(
        "property_details",
        properties_details,
        schema="rightmove_intermediate",
        replace=True,
    )


@task(sources="intermediate.property_details", outputs="models.f_properties")
def extract_property_facts(
    context: Task,
    warehouse: Database,
):
    with context.step("Check if table exists:"):
        out_table = context.out("models.f_properties")
        # check if table exists
        warehouse.execute(
            f"""CREATE TABLE IF NOT EXISTS `{out_table}` (
                property_id	        STRING,
                property_url	    STRING,				
                location_name	    STRING,				
                rent_pcm	        INTEGER,	
                let_available_date	STRING,			
                deposit	            STRING,
                min_tenancy	        STRING,			
                let_type	        STRING,			
                furnish_type	    STRING,				
                property_type	    STRING,				
                bedrooms	        STRING,
                bathrooms	        STRING,			
                size	            STRING,
                epc_rating_url	    STRING,				
                title	            STRING,
                image	            STRING,		
                description	        STRING,	
                date_added	        TIMESTAMP,
                is_favourite        INTEGER,
                is_hidden           INTEGER	
            );
            """
        )

    with context.step("Merge new Properties"):
        src_table = context.src("intermediate.property_details")
        values = warehouse.read_data(f"SELECT * FROM {src_table}")
        df_new = pd.DataFrame(values).drop_duplicates(subset="property_id")

        df_old = pd.DataFrame(warehouse.read_data(f"SELECT * FROM {out_table}"))[
            ["property_id", "is_favourite", "is_hidden"]
        ]
        df_new = df_new.merge(df_old, on="property_id", how="left")
        df_new[["is_favourite", "is_hidden"]] = df_new[
            ["is_favourite", "is_hidden"]
        ].fillna(0)

        df_new["date_added"] = (
            df_new["date_added"].apply((lambda x: x.to_pydatetime())).astype(str)
        )

        output = df_new.to_dict("records")

        for o in output:
            o["date_added"] = datetime.strptime(
                o["date_added"], "%Y-%m-%d %H:%M:%S.%f%z"
            )

    with context.step("Load Database"):
        t_name = out_table.split(".")[1]
        s_name = out_table.split(".")[0]
        warehouse.load_data(
            t_name,
            output,
            schema=s_name,
            replace=True,
        )

    return context.success()
