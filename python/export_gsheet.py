from typing import Mapping
from google.oauth2 import service_account
import google.auth
from googleapiclient.discovery import build
from sayn import task
from sayn.tasks.task import Task
from sayn.database import Database
import pygsheets as gs
import pandas as pd


@task(sources="raw.property_details")
def extract_gsheet(
    context: Task,
    warehouse: Database,
    gsheets: Mapping[str, Mapping[str, Mapping[str, str]]],
):
    with context.step("Config"):
        if not (service_account_info := gsheets.get("service_account")):
            credentials, _ = google.auth.default(
                scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"]
            )
        else:
            credentials = service_account.Credentials.from_service_account_info(
                service_account_info,
                scopes=("https://www.googleapis.com/auth/spreadsheets",),
            )

    with context.step("Get data"):
        src_table = context.src("intermediate.property_details")
        values = warehouse.read_data(f"SELECT * FROM {src_table}")
        df_new = pd.DataFrame(values).drop_duplicates(subset="property_id")

    with context.step("Get Gsheets data"):
        # API connection
        # Auth

        gc = gs.authorize(custom_credentials=credentials)
        sh = gc.open_by_key(gsheets["sheets"]["rightmove"]["id"])

        wks = sh[1]

        df_old = wks.get_as_df()

        df_final = pd.concat([df_old, df_new], ignore_index=True).drop_duplicates(
            subset="property_id"
        )

        wks.clear()

        wks.set_dataframe(df_final, (1, 1))
