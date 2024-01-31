from langchain_core.output_parsers import StrOutputParser
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.runnables import RunnablePassthrough
from langchain_community.chat_models import ChatAnthropic
from langchain_community.chat_models import ChatOpenAI

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
model = ChatOpenAI(model="gpt-3.5-turbo")
chain = (
    {"topic": RunnablePassthrough()}
    | prompt
    | model
    | output_parser
)

fallback_chain = chain.with_fallbacks([anthropic_chain])

if __name__ == "__main__":
    print(fallback_chain.invoke("ice cream"))
