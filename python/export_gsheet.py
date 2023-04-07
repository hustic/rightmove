from typing import Mapping
from google.oauth2 import service_account
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
        service_account_info = gsheets["service_account"]

    with context.step("Get data"):
        src_table = context.src("intermediate.property_details")
        values = warehouse.read_data(f"SELECT * FROM {src_table}")
        df_new = pd.DataFrame(values).drop_duplicates(subset="property_id")

    with context.step("Get Gsheets data"):
        # API connection
        # Auth

        gsheet_credentials = service_account.Credentials.from_service_account_info(
            service_account_info,
            scopes=("https://www.googleapis.com/auth/spreadsheets",),
        )
        gc = gs.authorize(custom_credentials=gsheet_credentials)
        sh = gc.open_by_key(gsheets["sheets"]["rightmove"]["id"])

        wks = sh[1]

        df_old = wks.get_as_df()

        df_final = df_old.merge(df_new, on=["property_id"], how="left")

        wks.clear()

        wks.set_dataframe(df_final, (1, 1))
