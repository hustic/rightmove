from typing import TypedDict

import httpx
from bs4 import BeautifulSoup
from sayn import task
from sayn.database import Database
from sayn.tasks.task import Task
from utils import format_field_name

BASE_URL = "https://www.rightmove.co.uk/"


class propertyInfo(TypedDict):
    property_id: str
    property_url: str
    let_available_date: str
    deposit: str
    min_tenancy: str
    let_type: str
    furnish_type: str
    property_type: str
    bedrooms: str
    bathrooms: str
    size: str
    epc_rating_url: str


@task(outputs="raw.property_details")
def extract_property_details(context: Task, warehouse: Database):
    properties = warehouse.read_data(query="SELECT * FROM raw.property_links")
    with context.step("Get Propery Details"):
        properties_details = []
        for _property in properties:
            property_url = _property["property_url"]
            property_info: propertyInfo = {"property_id": _property["property_id"]}
            with httpx.Client(base_url=BASE_URL) as client:
                property_response = client.get(property_url)
                property_soup = BeautifulSoup(property_response.text, "html.parser")

            property_detail = property_soup.select("dl._2E1qBJkWUYMJYHfYJzUb_r div")
            for dl in property_detail:
                property_info[format_field_name(dl.dt.string)] = (
                    dl.dd.string
                    or dl.dd.span.next_sibling
                    or dl.dd.span.previous_sibling
                )
            property_type = property_soup.select("div._4hBezflLdgDMdFtURKTWh dl")
            for dl in property_type:
                property_info[format_field_name(dl.dt.string)] = dl.dd.string

            epc_rating = property_soup.select_one(
                "div._3BAkOrQAfGZMsQDtC0WdbO._3A8p_O-xNhCM7MwsZ_g0yj a"
            )
            property_info["epc_rating_url"] = epc_rating["href"] if epc_rating else None
            properties_details.append(property_info)

    warehouse.load_data("raw.property_details", properties_details)
