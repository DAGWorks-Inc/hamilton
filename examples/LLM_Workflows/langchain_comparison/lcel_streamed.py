from langchain_core.output_parsers import StrOutputParser
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.runnables import RunnablePassthrough
from langchain_openai import ChatOpenAI

prompt = ChatPromptTemplate.from_template("Tell me a short joke about {topic}")
output_parser = StrOutputParser()
model = ChatOpenAI(model="gpt-3.5-turbo")
chain = {"topic": RunnablePassthrough()} | prompt | model | output_parser

if __name__ == "__main__":
    for chunk in chain.stream("ice cream"):
        print(chunk, end="", flush=True)
