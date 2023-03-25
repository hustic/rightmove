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
    location_name: str
    location_id: str


@task(outputs="raw.property_links")
def extract_property_links(context: Task, warehouse: Database):
    locations = warehouse.read_data(
        "SELECT * FROM add table here"
    )  # TODO: Fill this in
    property_links: List[propertyURL] = []
    for location in locations:
        params = {
            "searchType": "RENT",
            "locationIdentifier": f"REGION{location['location_id']}",
            "maxPrice": 2000,
            "minBedrooms": 2,
            "includeLetAgreed": "false",
            "index": 0,
        }

        with context.step("Get Propery Links"):
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
                                "location_id": location["location_id"],
                                "location_name": location["location_name"],
                            }
                            property_links.append(property_url)
                        else:
                            page = False

                    params["index"] += 24

        warehouse.load_data("raw.property_links", property_links)
