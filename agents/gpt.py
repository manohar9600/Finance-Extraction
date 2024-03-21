import hashlib
from langchain_community.vectorstores import Chroma
from langchain.docstore.document import Document
from langchain_community.embeddings import HuggingFaceEmbeddings
import uuid
from langchain.prompts import PromptTemplate
from langchain_core.output_parsers import JsonOutputParser
from langchain_core.pydantic_v1 import BaseModel, Field
from extraction.db_functions import DBFunctions, MinioDBFunctions
from datetime import datetime
import json
from extraction.html_functions import get_pages_text
from langchain_anthropic import ChatAnthropic
from langchain_core.messages import HumanMessage, SystemMessage
from loguru import logger


llm = ChatAnthropic(
    anthropic_api_key="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", 
    model_name="claude-3-opus-20240229"
)


def get_answer(question):
    doc_info = get_doc_info(question)
    db = DBFunctions()
    company_id = db.get_company_id(doc_info['Ticker'])
    document_info = db.get_table_data('documents', filter_dict={
        'companyid': company_id, 'filetype': doc_info['Type'].replace('-', ''), 
        'period': doc_info['Period']})

    if document_info.empty:
        return ''
    document_folder = document_info.iloc[0]['folderlocation']

    minio_fns = MinioDBFunctions('secreports')
    metadata = json.loads(minio_fns.get_object(document_folder+'/metadata.json'))
    html = minio_fns.get_object(metadata['mainHTML'])
    pages = get_pages_text(html)
    relevant_docs = get_relevant_docs(pages, question)
    context = '\n'.join(relevant_docs)
    prompt = f"context:{context}, question:{question}"
    final_ans = llm.invoke([
        SystemMessage(content="You're helpful assistant. Answer question from given context"),
        HumanMessage(content=prompt)
    ]).content
    return final_ans


def get_doc_info(question):
    class QueryJson(BaseModel):
        Ticker: str = Field(description="ticker of the company mentioned")
        Type: str = Field(description="type of document required for this question")
        Period: str = Field(description="period of document required for this question. period should be quarterly notation or fiscal year notation. format examples: 2022Q1, 2022FY")
    
    parser = JsonOutputParser(pydantic_object=QueryJson)

    prompt = PromptTemplate(
        template="Answer the user query.\n{format_instructions}\n{query}\n",
        input_variables=["query"],
        partial_variables={"format_instructions": parser.get_format_instructions()},
    )

    chain = prompt | llm | parser
    prompt = f"context: {question}, fill values required to in json"

    query_data = chain.invoke({"query": prompt})
    ticker = query_data['Ticker']
    db_conn = DBFunctions()
    company_info = db_conn.get_company_info(ticker)

    prompt = f"question: {question} today's date: {datetime.now().strftime('%B %d, %Y')} fiscal year-end (MM-DD): {company_info['Fiscal Year']} what are details of document required to answer above question."
    req_doc_info = chain.invoke({"query": prompt})
    req_doc_info['Ticker'] = ticker
    return req_doc_info


    # prompt = f"context: {question}, what is the ticker for mentioned company in above context. Output should contain only ticker"
    # ticker = llm.invoke([HumanMessage(content=prompt)]).content
    
    # latest_file = get_latest_file(f'data/Current/{ticker}')
    # with open(latest_file, 'r') as f:
    #     html_content = f.read()
    # pages = get_pages_text(html_content)

    # relevant_docs = get_relevant_docs(pages, question)
    # context = '\n'.join(relevant_docs)
    # prompt = f"context:{context}, question:{question}"
    # final_ans = llm.invoke([
    #     SystemMessage(content="You're helpful assistant. Answer question from given context"),
    #     HumanMessage(content=prompt)
    # ]).content
    # return final_ans


def get_relevant_docs(pages, question):
    collection_name = str(uuid.uuid4())
    vectorstore = Chroma(
        collection_name=collection_name,
        embedding_function=HuggingFaceEmbeddings(model_name="WhereIsAI/UAE-Large-V1"),
        # persist_directory="/home/manu/Data/vectorcache"
    )
    documents = [Document(page_content=s, metadata={'page':i+1}) for i, s in enumerate(pages)]
    if not vectorstore._collection.count():
        vectorstore.add_documents(documents)

    retriever = vectorstore.as_retriever(search_kwargs={"k": 10})
    relevant_docs = retriever.get_relevant_documents(question)
    relevant_docs = sorted(relevant_docs, key=lambda x: int(x.metadata['page']))
    logger.debug(f"RAG selected pages: {','.join([str(x.metadata['page']) for x in relevant_docs])}")
    relevant_docs = [d.page_content for d in relevant_docs]
    return relevant_docs
