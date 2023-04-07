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
    """Extract property details from rightmove

    Args:
        context (Task): Task context
        warehouse (Database): Database connection

    """

    query = f"""
        SELECT * FROM {context.src("raw.property_links")}
        WHERE date_added = (SELECT MAX(date_added) FROM {context.src("raw.property_links")})
    """

    properties = warehouse.read_data(query=query)

    with context.step("Get Propery Details"):
        properties_details = []
        for property in properties:
            if not isinstance(property, dict):
                raise Exception("Property not a dict")
            property_url = property["property_url"]
            property_info = template.copy()
            property_info["property_id"] = property["property_id"]
            property_info["property_url"] = BASE_URL[:-1] + property_url
            property_info["location_name"] = property["location_name"]
            try:
                with httpx.Client(base_url=BASE_URL) as client:
                    property_response = client.get(property_url)
                    property_soup = BeautifulSoup(property_response.text, "html.parser")

                rent_pcm = (
                    property_soup.select_one("div._1gfnqJ3Vtd1z40MlC0MzXu span")
                    .text.replace("£", "")
                    .replace("pcm", "")
                    .replace(",", "")
                    .strip()
                )
            except Exception as e:
                context.log.error(f"Error scraping property {property_url}")
                context.log.error(f"Error was: {e}")

            if rent_pcm is None:
                context.log.error(f"No rent found for property {property_url}")
                continue
            if not rent_pcm.isdigit():
                context.log.error(f"Found non-numeric rent for property {property_url}")
                continue

            yield {
                "property_url": property_url,
                "rent_pcm": int(rent_pcm),
                "date": datetime.datetime.now(),
            }

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
            # replace Ask agent with None and turn it into an integer
            if property_info["deposit"] == "Ask agent":
                property_info["deposit"] = None  # type: ignore

            else:
                property_info["deposit"] = int(property_info["deposit"])

            if property_info["min_tenancy"] == "Ask agent":
                property_info["min_tenancy"] = None

            property_info["bedrooms"] = property_info["bedrooms"].replace("U+0078", "")
            property_info["bathrooms"] = property_info["bathrooms"].replace(
                "U+0078", ""
            )
            property_info["size"] = property_info["size"].replace(" sq ft", "")
            # append to properties details when let_available_date is Now
            if property_info["let_available_date"] in ("Now", "Ask agent"):
                if property_info["let_available_date"] == "Now":
                    properties_details.append(property_info)
            else:
                if (
                    # month number of let available date is greater than 5
                    int(property_info["let_available_date"].split("-")[1])
                    > 5
                ):
                    properties_details.append(property_info)

    warehouse.load_data("property_details", properties_details, replace=True)
