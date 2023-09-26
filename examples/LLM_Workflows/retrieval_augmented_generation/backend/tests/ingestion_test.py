from hamilton import driver
import pytest

from backend import ingestion


def _get_embeddings__openai(
    texts: list[str], embedding_model_name: str
) -> list[list[float]]:
    """Mock of ingestion._get_embeddings__openai()"""
    return [[0, 1, 2, 3] for _ in range(len(texts))]


@pytest.fixture
def hamilton_driver(override_module_functions):
    """Hamilton driver with overriden functions"""
    ingestion_overriden = override_module_functions(ingestion, functions=[_get_embeddings__openai])
    return (
        driver.Builder()
        .enable_dynamic_execution(allow_experimental_mode=True)
        .with_modules(ingestion_overriden)
        .build()
    )


def test_local_pdf_to_raw_text(hamilton_driver, request):
    """Test flow path (`pdf_file`) -> (`raw_text`)"""
    # arrange
    pdf_absolute_path = request.path.parent.joinpath("./inputs/sample.pdf")
    overrides = dict(pdf_file=str(pdf_absolute_path))
    # act
    results = hamilton_driver.execute(final_vars=["raw_text", "file_name"], overrides=overrides)
    # assert
    assert isinstance(results.get("raw_text"), str) 
    

def test_raw_text_to_chunks(hamilton_driver):
    """Test flow path (`raw_text`) -> (`chunked_text`, `chunked_embeddings`)"""
    # arrange
    inputs = dict(embedding_model_name="text-embedding-ada-002")
    overrides = dict(
        raw_text="""Lorem Ipsum is simply dummy text of the printing and typesetting industry.
        Lorem Ipsum has been the industry's standard dummy text ever since the 1500s, when an
        unknown printer took a galley of type and scrambled it to make a type specimen book.
        It has survived not only five centuries, but also the leap into electronic typesetting,
        remaining essentially unchanged. It was popularised in the 1960s with the release of Letraset
        sheets containing Lorem Ipsum passages, and more recently with desktop publishing software
        like Aldus PageMaker including versions of Lorem Ipsum.\n
        """ * 5
    )
    # act
    results = hamilton_driver.execute(
        final_vars=["chunked_text", "chunked_embeddings"],
        inputs=inputs,
        overrides=overrides,
    )
    # assert
    assert len(results["chunked_text"]) == len(results["chunked_embeddings"])

