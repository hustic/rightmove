from sayn import task
from sayn.tasks.task import Task
from sayn.database import Database
from datetime import datetime
from bs4 import BeautifulSoup
import httpx

BASE_URL = "https://www.rightmove.co.uk/"


@task(sources="raw.rightmove_locations", outputs="raw.property_links")
def extract_property_links(context: Task, warehouse: Database):
    src_table = context.src("raw.rightmove_locations")
    locations = warehouse.read_data(f"SELECT * FROM {src_table}")

    property_links = []
    today = datetime.now()
    context.set_run_steps(
        [f"Get Property Links for {loc['location_name']}" for loc in locations]
    )

    params = {
        "searchType": "RENT",
        "maxPrice": 2000,
        "minBedrooms": 2,
        "includeLetAgreed": "false",
    }

    with httpx.Client(base_url=BASE_URL, params=params) as client:
        for location in locations:
            index = 0
            context.start_step(f"Get Property Links for {location['location_name']}")
            page = True
            while page:
                try:
                    response = client.get(
                        "property-to-rent/find.html",
                        params={
                            "locationIdentifier": f"REGION{location['location_id']}",
                            "index": index,
                        },
                    )

                except Exception as e:
                    context.info(e)
                    continue

                soup = BeautifulSoup(response.text, "html.parser")
                links = soup.select(".propertyCard-link")
                links = list(set([link["href"] for link in links]))
                for link in links:
                    if link != "":
                        property_url = {
                            "property_id": link.split("/")[2],
                            "property_url": link,
                            "location_id": location["location_id"],
                            "location_name": location["location_name"],
                            "date_added": today,
                        }
                        property_links.append(property_url)
                    else:
                        page = False

                index += 24
                context.finish_current_step()

    warehouse.load_data("property_links", property_links, schema="rightmove_raw")
