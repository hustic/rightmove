from itertools import groupby
from typing import Any, Mapping, Union
from google.oauth2 import service_account
import google.auth
from google.cloud import bigquery
from googleapiclient.discovery import build
from sayn import task
from sayn.tasks.task import Task
from sayn.database import Database
import datetime
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
        credentials, _ = google.auth.default(
            scopes=["https://www.googleapis.com/auth/spreadsheets"]
        )
        gsheets_api = build("sheets", "v4", credentials=credentials)

    with context.step("Get data"):
        src_table = context.src("intermediate.property_details")
        values = warehouse.read_data(f"SELECT * FROM {src_table}")
        df = pd.DataFrame(values).drop_duplicates(subset="property_id")

    with context.step("Get Gsheets data"):
        # API connection
        # Auth
        
        gsheet_credentials = service_account.Credentials.from_service_account_info(
            service_account_info,
            scopes=("https://www.googleapis.com/auth/spreadsheets",),
        )
        gc = gs.authorize(custom_credentials=gsheet_credentials)
        sh = gc.open_by_key('15lFBaIpoPkGeWjp3nHhxdKt11gqfI2xqN3v42DCtmFA')

        wks = sh[1]

        wks.set_dataframe(df, (1, 1))
   