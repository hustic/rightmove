from datetime import datetime

import httpx
from bs4 import BeautifulSoup
from sayn import task
from sayn.database import Database
from sayn.tasks.task import Task
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
    "date_added": datetime.today(),
}


@task(sources="raw.property_links", outputs="raw.property_details")
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
            property_info["property_id"] = property["property_id"]
            property_info["property_url"] = BASE_URL[:-1] + property_url
            property_info["location_name"] = _property["location_name"]

            property_response = client.get(property_url)
            property_soup = BeautifulSoup(property_response.text, "html.parser")
            rent_pcm = (
                property_soup.select_one("div._1gfnqJ3Vtd1z40MlC0MzXu span")
                .text.replace("£", "")
                .replace("pcm", "")
                .replace(",", "")
                .strip()
            )
            property_info["rent_pcm"] = int(rent_pcm)
            property_detail = property_soup.select("dl._2E1qBJkWUYMJYHfYJzUb_r div")
            for dl in property_detail:
                property_info[format_field_name(dl.dt.string)] = str(
                    dl.dd.string
                    or dl.dd.span.next_sibling
                    or dl.dd.span.previous_sibling
                )
            property_type = property_soup.select("div._4hBezflLdgDMdFtURKTWh dl")
            for dl in property_type:
                property_info[format_field_name(dl.dt.string)] = str(dl.dd.string)

            epc_rating = property_soup.select_one(
                "div._3BAkOrQAfGZMsQDtC0WdbO._3A8p_O-xNhCM7MwsZ_g0yj a"
            )
            property_info["epc_rating_url"] = str(
                epc_rating["href"] if epc_rating else None
            )
            property_info["deposit"] = property_info["deposit"].replace(",", "")
            property_info["bedrooms"] = property_info["bedrooms"].replace("U+00D7", "")
            property_info["bathrooms"] = property_info["bathrooms"].replace(
                "U+00D7", ""
            )
            if property_info["let_available_date"] == "Now":
                properties_details.append(property_info)
            elif property_info["let_available_date"] not in ("Ask agent"):
                if int(property_info["let_available_date"].split("/")[1]) in (6, 7):
                    properties_details.append(property_info)

            if stp == 49:
                context.finish_current_step()

    warehouse.load_data(
        "property_details",
        properties_details,
        schema="rightmove_intermediate",
        replace=True,
    )
