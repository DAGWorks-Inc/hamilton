from langchain_core.output_parsers import StrOutputParser
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.runnables import RunnablePassthrough
from langchain_openai import OpenAI

prompt = ChatPromptTemplate.from_template("Tell me a short joke about {topic}")
output_parser = StrOutputParser()
llm = OpenAI(model="gpt-3.5-turbo-instruct")
llm_chain = {"topic": RunnablePassthrough()} | prompt | llm | output_parser

if __name__ == "__main__":
    print(llm_chain.invoke("ice cream"))
