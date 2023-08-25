from hamilton import (
    Chain,
    DataFrame,
    Function,
    Input,
    Output,
    Source,
    Text,
)

from langchain.chains.llm import LLMChain
from langchain.chains.openai_functions.utils import get_llm_kwargs
from langchain.output_parsers.openai_functions import (
    PydanticOutputFunctionsParser,
)
from langchain.prompts.chat import ChatPromptTemplate, HumanMessagePromptTemplate
from langchain.pydantic_v1 import BaseModel, Field
from langchain.schema.language_model import BaseLanguageModel
from langchain.schema.messages import HumanMessage, SystemMessage

def create_citation_fuzzy_match_chain(llm: BaseLanguageModel) -> Chain:
    """Create a citation fuzzy match chain.

    Args:
        llm: Language model to use for the chain.

    Returns:
        Chain (LLMChain) that can be used to answer questions with citations.
    """

    output_parser = PydanticOutputFunctionsParser(pydantic_schema=QuestionAnswer)
    schema = QuestionAnswer.schema()
    function = Function(
        name=schema["title"],
        description=schema["description"],
        parameters=schema,
    )
    llm_kwargs = get_llm_kwargs(function)

    # Create the dataflow

    chain = Chain()

    # Create the input

    context = Input(name="context", type=Text)
    question = Input(name="question", type=Text)

    # Create the LLM step

    llm_step = LLMChain(
        llm=llm,
        prompt=ChatPromptTemplate(messages=[SystemMessage(content="")]),
        llm_kwargs=llm_kwargs,
        output_parser=output_parser,
    )

    # Connect the steps

    chain.connect(context, llm_step.input["context"])
    chain.connect(question, llm_step.input["question"])

    # Return the chain

    return chain

