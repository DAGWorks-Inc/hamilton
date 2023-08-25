from hamilton import (
    Chain,
    DataFrame,
    Function,
    Input,
    Output,
    Source,
    Text,
)


def create_citation_fuzzy_match_chain(llm: BaseLanguageModel) -> Chain:
  

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

