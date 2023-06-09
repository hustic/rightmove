from typing import Mapping
from google.oauth2 import service_account
import google.auth
from googleapiclient.discovery import build
from sayn import task
from sayn.tasks.task import Task
from sayn.database import Database
import datetime


_elt_ts = datetime.datetime.now()


@task(outputs="raw.rightmove_locations")
def extract_locations(
    context: Task,
    warehouse: Database,
    gsheets: Mapping[str, Mapping[str, Mapping[str, str]]],
):
    with context.step("Config"):
        # Gsheet information
        gsheet_info = gsheets["sheets"]["rightmove"]
        sheet_id = gsheet_info["id"]
        sheet_name = gsheet_info["sheet_name"]

        if not (service_account_info := gsheets.get("service_account")):
            credentials, _ = google.auth.default(
                scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"]
            )
        else:
            credentials = service_account.Credentials.from_service_account_info(
                service_account_info or credentials,
                scopes=("https://www.googleapis.com/auth/spreadsheets.readonly",),
            )

    with context.step("Get Gsheets data"):
        # API connection
        # Auth
        gsheets_api = build("sheets", "v4", credentials=credentials)
        # Get data
        res = (
            gsheets_api.spreadsheets()
            .values()
            .get(spreadsheetId=sheet_id, range=sheet_name)
            .execute()
        )
        data = [dict(zip(res["values"][0], d)) for d in res["values"][1:]]
        data = [dict(d, **{"_elt_ts": _elt_ts}) for d in data]

    with context.step("Load Database"):
        schema = "rightmove_raw"
        table = "rightmove_locations"
        warehouse.load_data(table, data, schema=schema, replace=True)
