from operator import itemgetter

from langchain.prompts.prompt import PromptTemplate
from langchain.schema import format_document
from langchain_community.vectorstores import FAISS
from langchain_core.messages import AIMessage, HumanMessage, get_buffer_string
from langchain_core.output_parsers import StrOutputParser
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.runnables import RunnableParallel, RunnablePassthrough
from langchain_openai import ChatOpenAI, OpenAIEmbeddings

vectorstore = FAISS.from_texts(["harrison worked at kensho"], embedding=OpenAIEmbeddings())
retriever = vectorstore.as_retriever()

_template = """Given the following conversation and a follow up question, rephrase the follow up question to be a standalone question, in its original language.

Chat History:
{chat_history}
Follow Up Input: {question}
Standalone question:"""
CONDENSE_QUESTION_PROMPT = PromptTemplate.from_template(_template)

template = """Answer the question based only on the following context:
{context}

Question: {question}
"""
ANSWER_PROMPT = ChatPromptTemplate.from_template(template)

DEFAULT_DOCUMENT_PROMPT = PromptTemplate.from_template(template="{page_content}")


def _combine_documents(docs, document_prompt=DEFAULT_DOCUMENT_PROMPT, document_separator="\n\n"):
    doc_strings = [format_document(doc, document_prompt) for doc in docs]
    return document_separator.join(doc_strings)


_inputs = RunnableParallel(
    standalone_question=RunnablePassthrough.assign(
        chat_history=lambda x: get_buffer_string(x["chat_history"])
    )
    | CONDENSE_QUESTION_PROMPT
    | ChatOpenAI(temperature=0)
    | StrOutputParser(),
)
_context = {
    "context": itemgetter("standalone_question") | retriever | _combine_documents,
    "question": lambda x: x["standalone_question"],
}
conversational_qa_chain = _inputs | _context | ANSWER_PROMPT | ChatOpenAI()

print(
    conversational_qa_chain.invoke(
        {
            "question": "where did harrison work?",
            "chat_history": [],
        }
    )
)
print(
    conversational_qa_chain.invoke(
        {
            "question": "where did he work?",
            "chat_history": [
                HumanMessage(content="Who wrote this notebook?"),
                AIMessage(content="Harrison"),
            ],
        }
    )
)
