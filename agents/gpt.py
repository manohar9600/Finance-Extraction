import os
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
from langchain_core.messages import HumanMessage, SystemMessage
from langchain_core.output_parsers.openai_tools import JsonOutputToolsParser
from langchain_anthropic.experimental import ChatAnthropicTools
from loguru import logger

llm = ChatAnthropicTools(
    anthropic_api_key="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",
    model="claude-3-sonnet-20240229")


def download_file(ticker: str, document_type: str, period: str, question:str) -> str:
    """Function fetches the file from the location and returns the file to user. This function is called when user asks to download the file.

    Args:
        ticker: ticker of the company
        document_type: type of document required for this question, either 10K or 10Q or others
        period: period of document required for this question. period should be quarterly notation or fiscal year notation. format examples: 2022Q1, 2022FY
    """
    db = DBFunctions()
    company_id = db.get_company_id(ticker)
    document_info = db.get_table_data('documents', filter_dict={
        'companyid': company_id, 'filetype': document_type.replace('-', ''), 
        'period': period})

    if document_info.empty:
        return ''
    document_folder = document_info.iloc[0]['folderlocation']

    minio_fns = MinioDBFunctions('secreports')
    metadata = json.loads(minio_fns.get_object(document_folder+'/metadata.json'))
    return os.path.join("http://"+minio_fns.url, 'secreports', metadata['mainHTML'])


def get_answer(ticker: str, document_type: str, period: str, question:str) -> str:
    """Function processes the file and returns the answer to the user. This function is called when user asks to get the answer.

    Args:
        ticker: ticker of the company
        document_type: type of document required for this question, either 10K or 10Q or others
        period: period of document required for this question. period should be quarterly notation or fiscal year notation. format examples: 2022Q1, 2022FY
    """
    db = DBFunctions()
    company_id = db.get_company_id(ticker)
    document_info = db.get_table_data('documents', filter_dict={
        'companyid': company_id, 'filetype': document_type.replace('-', ''), 
        'period': period})

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


def process_question(question):
    company_info = get_company_info(question)
    prompt = f"avaible documents: \n{company_info['docs'].to_markdown()} \n\n question: {question} \n today's date: {datetime.now().strftime('%B %d, %Y')} \n fiscal year-end (MM-DD): {company_info['Fiscal Year']} \n Call relevant function to follow above question."
    llm_with_tools = llm.bind_tools([download_file, get_answer])
    tool_chain = llm_with_tools | JsonOutputToolsParser()
    response = tool_chain.invoke(prompt)
    print(response)

    available_functions = {
        "download_file": download_file,
        "get_answer": get_answer
    }

    answer = ''
    for tool in response:
        fn_to_call = available_functions[tool['type']]
        tool['args']['question'] = question
        answer += fn_to_call(**tool['args'])
    return answer


def get_company_info(question):
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
    logger.info(f"company: {company_info['Name']}")
    company_info['docs'] = db_conn.get_table_data(
        "documents", filter_dict={"companyid": company_info['id']})
    return company_info


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
