import fastapi

from hamilton.experimental import h_async

from . import async_module

app = fastapi.FastAPI()


@app.post("/execute")
async def call(request: fastapi.Request) -> dict:
    """Handler for pipeline call"""
    dr = h_async.AsyncDriver({}, async_module)
    input_data = {"request": request}
    return await dr.raw_execute(["pipeline"], inputs=input_data)
