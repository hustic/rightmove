from sayn import task
from sayn.tasks.task import Task
from sayn.database import Database
from datetime import datetime
from bs4 import BeautifulSoup
import httpx
import re

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
        "maxPrice": 2500,
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
                    response.raise_for_status()

                except Exception as e:
                    context.info(e)
                    continue
                soup = BeautifulSoup(response.text, "html.parser")

                properties = soup.find_all(
                    "div",
                    id=re.compile(r"property-[1-9]\d+"),
                    class_=["l-searchResult", "is-list"],
                )
                if not properties or len(properties) < 25:
                    page = False

                for _property in properties:
                    property_link = _property.find("a", class_="propertyCard-link")
                    property_link = property_link["href"]

                    property_id = property_link.split("/")[2] if property_link else ""
                    # Extract small image URL
                    small_image = _property.find("img", itemprop="image")["src"]

                    # Extract title
                    title = _property.find(
                        "h2", class_="propertyCard-title"
                    ).text.strip()

                    # Extract description
                    description = _property.find(
                        "span", itemprop="description"
                    ).text.strip()

                    property_url = {
                        "property_id": property_id,
                        "property_url": property_link,
                        "location_id": location["location_id"],
                        "location_name": location["location_name"],
                        "image": small_image,
                        "description": description,
                        "title": title,
                        "date_added": today,
                    }
                    property_links.append(property_url)

                index += 24
                context.finish_current_step()

    warehouse.load_data("property_links", property_links, schema="rightmove_raw")
