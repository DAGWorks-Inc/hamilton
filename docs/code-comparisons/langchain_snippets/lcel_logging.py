from langchain_core.output_parsers import StrOutputParser
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.runnables import RunnablePassthrough
from langchain_community.chat_models import ChatAnthropic

prompt = ChatPromptTemplate.from_template(
    "Tell me a short joke about {topic}")
output_parser = StrOutputParser()
anthropic = ChatAnthropic(model="claude-2")
anthropic_chain = (
    {"topic": RunnablePassthrough()}
    | prompt
    | anthropic
    | output_parser
)


if __name__ == "__main__":
    import os
    os.environ["LANGCHAIN_API_KEY"] = "..."
    os.environ["LANGCHAIN_TRACING_V2"] = "true"
    # it's hard to customize the logging output of langchain
    # so here's their way to try to make money from you!
    print(anthropic_chain.invoke("ice cream"))
