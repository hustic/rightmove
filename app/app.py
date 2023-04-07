import logging

import dash_bootstrap_components as dbc
import google.auth
import pygsheets as gs
from dash import Dash, html

logger = logging.getLogger("gunicorn.error")
logger.setLevel(logging.DEBUG)
logger.addHandler(logging.StreamHandler())

external_stylesheets = [dbc.themes.BOOTSTRAP]

app = Dash(__name__, external_stylesheets=external_stylesheets)

server = app.server

credentials, _ = google.auth.default(
    scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"]
)
gc = gs.authorize(custom_credentials=credentials)


sh = gc.open_by_key("15lFBaIpoPkGeWjp3nHhxdKt11gqfI2xqN3v42DCtmFA")

wks = sh[1]

df = wks.get_as_df()

df["property_url"] = df["property_url"].apply(
    lambda x: html.A("Click me", href=x, target="_blank")
)

# wrap epc_rating_url values in html.A if they are not None
df["epc_rating_url"] = df["epc_rating_url"].apply(
    lambda x: html.A("Click me", href=x, target="_blank") if x else None
)
# add column containing dbc checkboxes named keep (default to True) with id set to property_id
df["shortlist"] = df["property_id"].apply(lambda x: dbc.Checkbox(id=x, value=True))

df = df.sort_values(by="rent_pcm", ascending=False)

app.layout = html.Div(
    [
        dbc.Row(
            [
                dbc.Col(
                    [
                        html.H1("Rightmove Property Scraper"),
                        html.Hr(),
                        # dbc.Table.from_dataframe(
                        #     df,
                        #     striped=True,
                        #     bordered=True,
                        #     hover=True,
                        #     style={"width": "100%"},
                        #     responsive=True,
                        # ),
                    ],
                    md=12,
                ),
            ],
            align="center",
        )
    ],
    # add 5% padding to all sides
    style={"padding": "5%"},
)


if __name__ == "__main__":
    app.run_server(host="0.0.0.0", port=8050, debug=True)
