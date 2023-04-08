import os
import logging
import dash_bootstrap_components as dbc
import google.auth
import pygsheets as gs
from dash import Dash, html
from functools import lru_cache

logger = logging.getLogger("gunicorn.error")
logger.setLevel(logging.DEBUG)
logger.addHandler(logging.StreamHandler())


external_stylesheets = [
    dbc.themes.BOOTSTRAP,
    "https://fonts.googleapis.com/css2?family=Material+Symbols+Outlined:opsz,wght,FILL,GRAD@20..48,100..700,0..1,-50..200",
]

app = Dash(__name__, external_stylesheets=external_stylesheets)

server = app.server

API_KEY = os.getenv("API_KEY")


@lru_cache
def get_dataframe():
    credentials, _ = google.auth.default(
        scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"]
    )
    gc = gs.authorize(custom_credentials=credentials)
    sh = gc.open_by_key("15lFBaIpoPkGeWjp3nHhxdKt11gqfI2xqN3v42DCtmFA")
    wks = sh[1]
    df = wks.get_as_df()

    return df


df = get_dataframe()
df = df.sort_values(by="rent_pcm", ascending=False)

N_PROPS = len(df)
COLS = 5
ROWS = N_PROPS // COLS + 1
rows = []

for row in range(ROWS):
    _row = []
    for col in range(COLS):
        idx = row * 4 + col
        if idx > N_PROPS - 1:
            break
        _col = dbc.Col(
            [
                dbc.Card(
                    [
                        dbc.CardImg(src=df["image"][idx], top=True),
                        dbc.CardBody(
                            [
                                html.H4(df["title"][idx], className="card-title"),
                                html.Div(
                                    className="property-details",
                                    children=[
                                        html.Span(
                                            "bed",
                                            className="material-symbols-outlined",
                                        ),
                                        html.Span(df["bedrooms"][idx]),
                                        html.Span(
                                            "shower",
                                            className="material-symbols-outlined",
                                        ),
                                        html.Span(df["bathrooms"][idx]),
                                        html.Span(
                                            "payments",
                                            className="material-symbols-outlined",
                                        ),
                                        html.Span(f'£{df["rent_pcm"][idx]}'),
                                    ],
                                ),
                                html.Span(
                                    f"Available from: {df['let_available_date'][idx]}"
                                ),
                                html.Br(),
                                html.Span(f"Location: {df['location_name'][idx]}"),
                                html.P(
                                    df["description"][idx],
                                    className="card-text",
                                ),
                                dbc.Button(
                                    "Go to property",
                                    href=df["property_url"][idx],
                                    target="_blank",
                                    color="primary",
                                ),
                            ]
                        ),
                    ],
                    style={"width": "18rem"},
                    class_name="property-card",
                ),
            ]
        )
        _row.append(_col)
    rows.append(dbc.Row(_row, style={"margin-bottom": "1rem"}))


app.layout = html.Div(
    [
        html.H1("Properties"),
        html.Hr(),
        dbc.Row(rows),
    ],
    # add 5% padding to left-right 2% padding to top-bottom
    style={"padding": "2% 5%"},
)


if __name__ == "__main__":
    app.run_server(host="0.0.0.0", port=8050, debug=True)
