import logging
import os
from functools import lru_cache

import dash_bootstrap_components as dbc
import pandas as pd
from dash import MATCH, Dash, Input, Output, State, ctx, html, no_update
from dash.exceptions import PreventUpdate

from .data_access import DataAccess

logger = logging.getLogger("gunicorn.error")
logger.setLevel(logging.DEBUG)
logger.addHandler(logging.StreamHandler())


external_stylesheets = [
    dbc.themes.BOOTSTRAP,
    "https://fonts.googleapis.com/css2?family=Material+Symbols+Outlined\
        :opsz,wght,FILL,GRAD@20..48,100..700,0..1,-50..200",
]

app = Dash(__name__, external_stylesheets=external_stylesheets)

server = app.server


@lru_cache
def data_access() -> DataAccess:
    return DataAccess(
        project_id=os.environ["PROJECT_ID"],
        dataset_id=os.environ["DATASET_ID"],
    )


df = data_access().get_dataframe(table_id="f_properties")
mask = df["is_hidden"] == 0
df = df[mask].sort_values(by="rent_pcm", ascending=False)


def make_card(
    idx: str,
    image_src: str,
    title: str,
    bedrooms: int,
    bathrooms: int,
    rent_pcm: int,
    let_available_date: str,
    location_name: str,
    description: str,
    property_url: str,
    is_favourite: int,
):
    style = (
        {
            "font-variation-settings": "'FILL' 1, 'wght' 400, 'GRAD' 0, 'opsz' 40",
            "color": "pink",
        }
        if is_favourite == 1
        else None
    )

    card = dbc.Card(
        [
            dbc.CardImg(src=image_src, top=True),
            dbc.CardBody(
                [
                    html.H4(title, className="card-title"),
                    html.Div(
                        className="property-details",
                        children=[
                            html.Span(
                                "bed",
                                className="material-symbols-outlined",
                            ),
                            html.Span(bedrooms),
                            html.Span(
                                "shower",
                                className="material-symbols-outlined",
                            ),
                            html.Span(bathrooms),
                            html.Span(
                                "payments",
                                className="material-symbols-outlined",
                            ),
                            html.Span(f"£{rent_pcm}"),
                        ],
                    ),
                    html.Span(f"Available from: {let_available_date}"),
                    html.Br(),
                    html.Span(f"Location: {location_name}"),
                    html.P(
                        description,
                        className="card-text",
                    ),
                    html.Div(
                        [
                            html.A(
                                html.Span(
                                    "open_in_new", className="material-symbols-outlined"
                                ),
                                href=property_url,
                                target="_blank",
                                className="property-card-button",
                            ),
                            html.A(
                                html.Span(
                                    "favorite",
                                    className="material-symbols-outlined",
                                    id={"type": "favourite-icon", "index": idx},
                                ),
                                href=None,
                                id={"type": "favourite-btn", "index": idx},
                                className="property-card-button",
                                style=style,
                            ),
                            html.A(
                                html.Span(
                                    "visibility",
                                    id={"type": "hide-icon", "index": idx},
                                    className="material-symbols-outlined",
                                ),
                                href=None,
                                id={"type": "hide-btn", "index": idx},
                                className="property-card-button",
                            ),
                        ]
                    ),
                ]
            ),
        ]
    )
    return card


def make_grid(df: pd.DataFrame, col_per_row: int = 5):
    N_PROPS = len(df)
    COLS = col_per_row
    ROWS = N_PROPS // COLS + 1
    rows = []
    ordered_fields = [
        "property_id",
        "image",
        "title",
        "bedrooms",
        "bathrooms",
        "rent_pcm",
        "let_available_date",
        "location_name",
        "description",
        "property_url",
        "is_favourite",
    ]
    for row in range(ROWS):
        _row = []
        for col in range(COLS):
            idx = row * COLS + col
            if idx > N_PROPS - 1:
                break
            _prop = df.iloc[idx]
            _col = dbc.Col(
                children=make_card(*_prop[ordered_fields].values),
                width=2,
                class_name="property-card",
            )
            _row.append(_col)
        rows.append(dbc.Row(_row, style={"margin-bottom": "1rem"}, justify="start"))

    return rows


app.layout = html.Div(
    [
        html.H1("Properties"),
        html.Hr(),
        dbc.Row(make_grid(df, col_per_row=6)),
    ],
    # add 5% padding to left-right 2% padding to top-bottom
    style={"padding": "2% 5%"},
)

# write callback function to fill favourite-btn when clicked


@app.callback(
    Output({"type": "favourite-icon", "index": MATCH}, "style"),
    Input({"type": "favourite-btn", "index": MATCH}, "n_clicks"),
    State({"type": "favourite-icon", "index": MATCH}, "style"),
    prevent_initial_call=True,
)
def favourite_property(n_clicks, style):
    if n_clicks is None:
        raise PreventUpdate
    else:
        if style:
            result = data_access().update_item(
                table_id="f_properties",
                property_id=ctx.triggered_id["index"],
                key="is_favourite",
                value="0",
            )
            if result:
                return {
                    "font-variation-settings": "'FILL' 0, 'wght' 400, 'GRAD' 0, 'opsz' 40",
                    "color": "#0d6efd",
                }
            else:
                return no_update
        else:
            result = data_access().update_item(
                table_id="f_properties",
                property_id=ctx.triggered_id["index"],
                key="is_favourite",
                value="1",
            )
            if result:
                return {
                    "font-variation-settings": "'FILL' 1, 'wght' 400, 'GRAD' 0, 'opsz' 40",
                    "color": "pink",
                }
            else:
                return no_update


@app.callback(
    Output({"type": "hide-icon", "index": MATCH}, "children"),
    Input({"type": "hide-btn", "index": MATCH}, "n_clicks"),
    prevent_initial_call=True,
)
def hide_property(n_clicks):
    if n_clicks is None:
        raise PreventUpdate
    else:
        property_id = ctx.triggered_id["index"]
        # is_hidden = True
        result = data_access().update_item(
            table_id="f_properties",
            property_id=property_id,
            key="is_hidden",
            value=str(n_clicks % 2),
        )
        if result:
            return "visibility_off" if n_clicks % 2 == 1 else "visibility"
        else:
            return no_update


if __name__ == "__main__":
    app.run_server(host="0.0.0.0", port=8050, debug=True)
