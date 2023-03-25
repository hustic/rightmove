import requests
from bs4 import BeautifulSoup
from urllib.parse import urlencode


BASE_URL = "https://www.rightmove.co.uk/property-to-rent/find.html?"
params = {
    "searchType": "RENT",
    "locationIdentifier": "REGION^1244",
    "maxPrice" : 2000 ,
    "minBedrooms" : 2,
    "propertyTypes" : "flats",
    # "primaryDisplayPropertyType" : "flats"
    # "sortType" : ,
    "includeLetAgreed": "false",
    "index": 0
}
page_number = 0
last_page = False
while not last_page:
    url = BASE_URL + urlencode(params)
    response = requests.get(url)

    soup = BeautifulSoup(response.text, "html.parser")

    property_links = soup.select(".propertyCard-link")
    print(len(property_links))
    for link in property_links:
        if link["href"] == "":
            last_page = True
            break
        property_url = "https://www.rightmove.co.uk" + link["href"]
        property_response = requests.get(property_url)
        property_soup = BeautifulSoup(property_response.text, "html.parser")
        let_available_date = property_soup.select_one("div._2RnXSVJcWbWv4IpBC1Sng6 dd").text.strip()
        # TODO(soto): make this dataframe

    
    params["index"] = params["index"] + 24
    page_number += 1

