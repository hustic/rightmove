from typing import TypedDict, List
from sayn import task
from sayn.tasks.task import Task
from sayn.database import Database

from bs4 import BeautifulSoup
import httpx

BASE_URL = "https://www.rightmove.co.uk/"


class propertyURL(TypedDict):
    property_id: str
    property_url: str


@task(outputs="raw.property_links")
def extract_property_links(context: Task, warehouse: Database):
    params = {
        "searchType": "RENT",
        "locationIdentifier": "REGION^1244",
        "maxPrice": 2000,
        "minBedrooms": 2,
        "propertyTypes": "flats",
        # "primaryDisplayPropertyType" : "flats"
        # "sortType" : ,
        "includeLetAgreed": "false",
        "index": 0,
    }

    with context.step("Get Propery Links"):
        property_links: List[propertyURL] = []

        with httpx.Client(base_url=BASE_URL) as client:
            page = True
            while page:
                response = client.get("property-to-rent/find.html", params=params)

                soup = BeautifulSoup(response.text, "html.parser")
                links = soup.select(".propertyCard-link")

                for link in links:
                    if (href := link["href"]) != "":
                        property_url: propertyURL = {
                            "property_id": href.split("/")[2],
                            "property_url": href,
                        }
                        property_links.append(property_url)
                    else:
                        page = False

                params["index"] += 24

        warehouse.load_data("raw.property_links", property_links)
