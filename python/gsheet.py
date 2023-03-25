from itertools import groupby
from typing import Any, Mapping, Union
# from google.oauth2 import service_account
import google.auth
from google.cloud import bigquery
from googleapiclient.discovery import build
from sayn import task
from sayn.tasks.task import Task
from sayn.database import Database

   


@task(outputs="raw.rightmove_locations")
def extract_gsheet_slack(
    context: Task,
    warehouse: Database,
    gsheets: Mapping[str, Mapping[str, Mapping[str, str]]],
):
    with context.step("Config"):
        # Schema object
        service_name = "gsheets"
        dataset_name = context.name[len("extract_gsheets") :]

        # Gsheet information
        gsheet_info = gsheets["sheets"][dataset_name]
        sheet_id = gsheet_info["id"]
        sheet_name = gsheet_info["sheet_name"]
        # append_mode = schema.config.get("append_mode", False)
        # if not isinstance(append_mode, bool):
        #     raise ValueError("append_mode should be a boolean value")
        # sheet_key = schema.config.get("key", list())
        # if not isinstance(sheet_key, list):
        #     raise ValueError("key should be a list")

        credentials, _ = google.auth.default(
            scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"]
        )
        gsheets_api = build("sheets", "v4", credentials=credentials)

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

    print(data)
   