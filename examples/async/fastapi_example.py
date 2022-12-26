import async_module
import fastapi

from hamilton import base
from hamilton.experimental import h_async

app = fastapi.FastAPI()

# can instantiate a driver once for the life of the app:
dr = h_async.AsyncDriver({}, async_module, result_builder=base.DictResult())


@app.post("/execute")
async def call(request: fastapi.Request) -> dict:
    """Handler for pipeline call"""
    input_data = {"request": request}
    # Can instantiate a driver within a request as well:
    # dr = h_async.AsyncDriver({}, async_module, result_builder=base.DictResult())
    result = await dr.execute(["pipeline"], inputs=input_data)
    return result


if __name__ == "__main__":
    # If you run this as a script, then the app will be started on localhost:8000
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
