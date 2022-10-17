import async_module
import fastapi

from hamilton.experimental import h_async

app = fastapi.FastAPI()


@app.post("/execute")
async def call(request: fastapi.Request) -> dict:
    """Handler for pipeline call"""
    dr = h_async.AsyncDriver({}, async_module)
    input_data = {"request": request}
    return await dr.raw_execute(["pipeline"], inputs=input_data)


if __name__ == "__main__":
    # If you run this as a script, then the app will be started on localhost:8000
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
