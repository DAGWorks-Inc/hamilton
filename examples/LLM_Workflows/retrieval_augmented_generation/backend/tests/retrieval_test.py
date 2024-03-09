import pytest

from backend import retrieval


def test_check_if_summary_exists():
    # arrange
    chunks = [
        dict(uuid=0, summary="summary A"),
        dict(uuid=1, name="John", summary="summary B"),
        dict(uuid=2, name="Kevin"),
    ]
    # act 
    output = retrieval.check_if_summary_exists(chunks)
    # assert
    chunks_without_summary: list = output["chunks_without_summary"]
    chunks_with_summary: list = output["chunks_with_summary"]
    assert len(chunks) == len(chunks_without_summary) + len(chunks_with_summary)


def test_prompt_template_to_summarize_chunk():
    """Test if prompt template has {content} field"""
    # arrange / act
    prompt_template = retrieval.prompt_template_to_summarize_chunk()
    # assert
    assert prompt_template.count("{content}") == 1


def test_prompt_to_summarize_chunk():
    """Test if prompt template has no unfilled {content} field"""
    # arrange
    chunk_without_summary = dict(
        content="Chunk text content"
    )
    prompt_template = "Prompt template: {content}"
    # act
    prompt = retrieval.prompt_to_summarize_chunk(chunk_without_summary, prompt_template)
    # assert
    assert "{content}" not in prompt
    assert prompt == "Prompt template: Chunk text content"


def test_prompt_template_to_reduce_summaries():
    """Test if prompt template has {query} and {chunks_summary} fields"""
    # arrange / act
    prompt_template = retrieval.prompt_template_to_reduce_summaries()
    # assert
    assert prompt_template.count("{query}") == 1
    assert prompt_template.count("{chunks_summary}") == 1


def test_prompt_to_reduce_summaries():
    """Test if prompt template has no unfilled {query} and {chunks_summary} fields"""
    # arrange
    rag_query = "Who is B?"
    chunks = [
        dict(uuid=0, summary="summary A."),
        dict(uuid=1, name="John", summary="summary B."),
        dict(uuid=2, name="Kevin", summary="summary C."),
    ]
    prompt_template = "Prompt template: {query}\n{chunks_summary}"
    #act
    prompt = retrieval.prompt_to_reduce_summaries(rag_query, chunks, prompt_template)
    #assert
    assert "{query}" not in prompt
    assert "{chunks_summary}" not in prompt
    assert prompt == "Prompt template: Who is B?\nsummary A. summary B. summary C."


# parametrized alternative to above tests
# arrange/act
@pytest.mark.parametrize(
    "prompt_template,fields",
    [
        (retrieval.prompt_template_to_summarize_chunk(), ["{content}"]),
        (retrieval.prompt_template_to_reduce_summaries(), ["{query}", "{chunks_summary}"]),
    ]
)
def test_prompt_has_fields(prompt_template: str, fields: list[str]):
    # assert
    for field in fields:
        assert prompt_template.count(field) == 1
