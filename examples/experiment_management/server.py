import functools
import json

import pandas as pd
from experiment_hook import JsonCache
from fastapi import FastAPI
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastui import AnyComponent, FastUI
from fastui import components as c
from fastui import prebuilt_html
from fastui.components.display import DisplayLookup, DisplayMode
from fastui.events import GoToEvent
from pydantic_models import NodeMaterializer, RunMetadata, model_from_values

CACHE_PATH = "/home/tjean/projects/dagworks/hamilton/examples/experiment_management/repository"
EXPERIMENT_PATH = "./experiments"


cache = JsonCache(cache_path=CACHE_PATH)
runs = [RunMetadata.model_validate_json(cache.read(run_id)) for run_id in cache.keys()]


# Swagger UI /docs are currently bugged for FastUI
app = FastAPI(docs_url=None, redoc_url=None)
app.mount("/experiments", StaticFiles(directory=EXPERIMENT_PATH), name="experiments")


def base_page(*components: AnyComponent) -> list[AnyComponent]:
    return [
        c.PageTitle(text="📝 Hamilton Experiment Manager"),
        c.Navbar(
            title="📝 Hamilton Experiment Manager",
            title_event=GoToEvent(url="/"),
            class_name="+ mb-4",
        ),
        c.Page(components=[*components]),
        c.Footer(
            extra_text="Powered by Hamilton",
            links=[
                c.Link(
                    components=[c.Text(text="Github")],
                    on_click=GoToEvent(url="https://github.com/dagworks-inc/hamilton"),
                ),
            ],
        ),
    ]


@functools.cache
def run_lookup():
    return {run.run_id: run for run in runs}


@app.get("/api/runs", response_model=FastUI, response_model_exclude_none=True)
def runs_overview() -> list[AnyComponent]:
    return base_page(
        c.Table(
            data=runs,
            data_model=RunMetadata,
            columns=[
                DisplayLookup(
                    field="date_completed", mode=DisplayMode.date, table_width_percent=10
                ),
                DisplayLookup(
                    field="run_id",
                    on_click=GoToEvent(url="/run/{run_id}/"),
                    table_width_percent=15,
                ),
            ],
        ),
    )


def dataframe_to_table(df: pd.DataFrame) -> AnyComponent:
    Table = model_from_values("Table", specs=df.iloc[0].to_json())
    data = [Table.model_validate(row) for row in df.to_dict(orient="index")]
    return c.Table(
        data=data,
        data_model=Table,
        columns=[DisplayLookup(field=str(col)) for col in df.columns],
    )


def get_materializer_component(materializer: NodeMaterializer) -> AnyComponent:
    if materializer.sink == "json":
        with open(materializer.path, "r") as f:
            data = json.load(f)
        component = c.Json(value=data)

    elif materializer.sink == "parquet":
        data = pd.read_parquet(materializer.path)
        component = dataframe_to_table(data)

    elif materializer.sink == "csv":
        data = pd.read_csv(materializer.path)
        component = dataframe_to_table(data)

    else:
        component = c.Json(
            value={
                k: v for k, v in dict(materializer).items() if k in ["path", "sink", "data_saver"]
            }
        )

    return component


def materializers_div(run: RunMetadata) -> list[AnyComponent]:
    materializers = []
    for materializer in run.materialized:
        materializers.append(
            c.Div(
                components=[
                    c.Heading(text="-".join(materializer.source_nodes), level=4),
                    get_materializer_component(materializer),
                ],
                class_name="my-2 p-2 border rounded",
            )
        )

    return materializers


@app.get("/api/run/{run_id}/", response_model=FastUI, response_model_exclude_none=True)
def run_details(run_id: str) -> list[AnyComponent]:
    run = run_lookup()[run_id]
    print(run.materialized)

    return base_page(
        c.Details(
            data=run,
            fields=[
                DisplayLookup(field="run_id"),
                DisplayLookup(field="success"),
                DisplayLookup(field="graph_hash"),
            ],
        ),
        c.Image(
            src=f"/experiments/{run.run_id}/dag.png",
            width="100%",
            height="auto",
            loading="lazy",
            referrer_policy="no-referrer",
            class_name="border rounded",
        ),
        c.Details(
            data=run,
            fields=[
                DisplayLookup(field="config", mode=DisplayMode.json),
                DisplayLookup(field="inputs", mode=DisplayMode.json),
                DisplayLookup(field="overrides", mode=DisplayMode.json),
            ],
        ),
        c.Heading(text="Results", level=3),
        c.Div(components=materializers_div(run)),
    )


@app.get("/api/")
def landing_page() -> RedirectResponse:
    return RedirectResponse(url="/api/runs")


@app.get("{path:path}")
async def html_landing() -> HTMLResponse:
    """Simple HTML page which serves the React app, comes last as it matches all paths."""
    return HTMLResponse(prebuilt_html(title="Hamilton Experiment Manager"))


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8080)
