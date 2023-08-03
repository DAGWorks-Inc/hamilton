import base64

import fastapi
import pydantic
import summarization

from hamilton import base
from hamilton.experimental import h_async

# instantiate FastAPI app
app = fastapi.FastAPI()


# define constants for Hamilton driver
driver_config = dict(
    openai_gpt_model="gpt-3.5-turbo-0613",
    file_type="pdf",
)

# instantiate the Hamilton driver; it will power all API endpoints
dr = h_async.AsyncDriver(
    driver_config,
    summarization,  # python module containing function logic
    result_builder=base.SimplePythonGraphAdapter(base.DictResult()),
)


class SummarizeResponse(pydantic.BaseModel):
    """Response to the /summarize endpoint"""

    summary: str


@app.post("/summarize")
async def summarize_pdf(pdf_file: fastapi.UploadFile) -> SummarizeResponse:
    """Request `summarized_text` from Hamilton driver with `pdf_file` and `user_query`"""
    results = await dr.execute(
        ["summarized_text"],
        inputs=dict(
            pdf_source=pdf_file.file,
            user_query="Can you ELI5 the paper?",
        ),
    )

    return SummarizeResponse(summary=results["summarized_text"])


# add to SwaggerUI the execution DAG png
# see http://localhost:8080/docs#/default/summarize_pdf_summarize_post
base64_viz = base64.b64encode(open("summarize_route.png", "rb").read()).decode("utf-8")
app.routes[
    -1
].description = f"""<h1>Execution DAG</h1><img alt="" src="data:image/png;base64,{base64_viz}"/>"""


if __name__ == "__main__":
    # run as a script to test server locally
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8080)
