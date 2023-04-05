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
    properties = warehouse.read_data(
        query="SELECT * FROM rightmove_raw.property_links WHERE date_added = MAX(date_added)"
    )

    with context.step("Get Propery Details"):
        properties_details = []
        for _property in properties:
            property_url = _property["property_url"]
            property_info = template.copy()
            property_info["property_id"] = _property["property_id"]
            property_info["property_url"] = BASE_URL[:-1] + property_url
            property_info["location_name"] = _property["location_name"]
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
            property_info["bedrooms"] = property_info["deposit"].replace("x", "")
            property_info["bathrooms"] = property_info["deposit"].replace("x", "")
            # print(property_info)
            if property_info["let_available_date"] not in ("Now", "Ask agent"):
                if (
                    datetime.strptime(
                        property_info["let_available_date"], "%d/%m/%Y"
                    ).month
                    > 5
                ):
                    properties_details.append(property_info)

    warehouse.load_data("property_details", properties_details, replace=True)
