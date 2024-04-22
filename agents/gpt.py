import os
import re
import hashlib
from langchain_community.vectorstores import Chroma
from langchain.docstore.document import Document
from langchain_community.embeddings import HuggingFaceEmbeddings
from langchain_openai import OpenAIEmbeddings
import uuid
from langchain.prompts import PromptTemplate
from langchain_core.output_parsers import JsonOutputParser
from langchain_core.pydantic_v1 import BaseModel, Field
from extraction.db_functions import DBFunctions, MinioDBFunctions, RedisFunctions
from datetime import datetime
import json
from extraction.html_functions import get_pages_text
from langchain_core.output_parsers.openai_tools import JsonOutputToolsParser
from extraction.gpt_functions import gpt4_llm, haiku_llm, get_haiku_answer
from loguru import logger

rd_fns = RedisFunctions()


class CustomOpenAIEmbeddings(OpenAIEmbeddings):

    def __init__(self, openai_api_key, *args, **kwargs):
        super().__init__(openai_api_key=openai_api_key, *args, **kwargs)
        
    def _embed_documents(self, texts):
        return super().embed_documents(texts)  # <--- use OpenAIEmbedding's embedding function

    def __call__(self, input):
        return self._embed_documents(input)    # <--- get the embeddings


class GPT:
    def __init__(self, uid=None) -> None:
        self.uid = uid
    
    def process_question(self, question):
        rd_fns.start_status(steps=150, uid=self.uid)
        rd_fns.update_status("Understanding the question", self.uid)
        class QuestionsJson(BaseModel):
            questions: list = Field(description="Breakdown of the question into individual questions, so that RAG can pick answers accurately. (field name should be 'question'). So that one single question can be asked to single document. Include type of question. It can be either 'information_request' or 'aggregator'. Aggregator questions should be last")
        
        parser = JsonOutputParser(pydantic_object=QuestionsJson)
        prompt = PromptTemplate(
            template="Answer the user query.\n{format_instructions}\n{query}\n",
            input_variables=["query"],
            partial_variables={"format_instructions": parser.get_format_instructions()},
        )
        chain = prompt | gpt4_llm | parser
        prompt = f"context: {question}, fill values required to in json."
        query_data = chain.invoke({"query": prompt})
        steps_count = 4*len([q for q in query_data['questions'] if q['type'] == 'information_request']) + 1
        rd_fns.start_status(steps=steps_count, uid=self.uid)
        messages = []
        sources = []
        for q in query_data['questions']:
            if q['type'] == 'information_request':
                answer = self.get_answer_doc(q['question'])
                if len(query_data['questions']) <= 1:
                    rd_fns.update_status("Final answer .... ", self.uid)
                    for ans in answer:
                        yield ans
                    break
                ans_chunks = list(answer)
                messages.append({"role": "user", "content": q['question']})
                messages.append({"role": "assistant", 
                                 "content": "".join([s['answer'] for s in ans_chunks])})
                sources.append(ans_chunks[0]['sources'][0])
            elif q['type'] == 'aggregator':
                rd_fns.update_status("Final answer .... ", self.uid)
                messages.append({"role": "user", "content": q['question']})
                # sources = list(set(sources))
                for ans in get_haiku_answer(messages):
                    yield {
                        'answer': ans,
                        'sources': sources
                    }
                break
        else:
            rd_fns.update_status("Final answer .... ", self.uid)
            messages.append({"role": "user", "content": question})
            for ans in get_haiku_answer(messages):
                yield {
                    'answer': ans,
                    'sources': sources
                }

    def get_answer_doc(self, question):
        rd_fns.update_status("Analysing company info", self.uid)
        company_info = get_company_info(question)
        if company_info is None:
            return 'Requested data is not yet available. Please try again later.'
        
        rd_fns.update_status(f"Searching for relevant documents from {company_info['Name']}", self.uid)
        prompt = f"avaible documents: \n{company_info['docs'].to_markdown()} \n\n question: {question} \n today's date: {datetime.now().strftime('%B %d, %Y')} \n fiscal year-end (MM-DD): {company_info['Fiscal Year']} \n Call relevant function to follow above question."
        llm_with_tools = gpt4_llm.bind_tools([self.download_file, self.get_answer])
        tool_chain = llm_with_tools | JsonOutputToolsParser()
        response = tool_chain.invoke(prompt)
        # print(response)

        available_functions = {
            "download_file": self.download_file,
            "get_answer": self.get_answer
        }

        # answer = ''
        # for tool in response:
        #     fn_to_call = available_functions[tool['type']]
        #     answer = answer + '\n' + fn_to_call(**tool['args'])
        return self.get_answer(**response[0]['args'])

    def download_file(self, ticker: str, document_type: str, period: str, question:str) -> str:
        """Function fetches the file from the location and returns the file to user. This function should be called when user specifically asks to download the file.

        Args:
            ticker: ticker of the company
            document_type: type of document required for this question, either 10K or 10Q or others
            period: period of document required for this question. period should be quarterly notation or fiscal year notation. format examples: 2022Q1, 2022FY
        """
        db = DBFunctions()
        company_id = db.get_company_id(ticker)
        document_info = db.get_table_data('documents', filter_dict={
            'companyid': company_id, 'ReportType': document_type.replace('-', ''), 
            'period': period})

        if document_info.empty:
            return 'Requested data is not yet available. Please try again later.'
        document_folder = document_info.iloc[0]['folderlocation']

        minio_fns = MinioDBFunctions('secreports')
        metadata = json.loads(minio_fns.get_object(document_folder+'/metadata.json'))
        return os.path.join("http://"+minio_fns.url, 'secreports', metadata['mainHTML'])

    def get_answer(self, ticker: str, document_type: str, period: str, question:str):
        """This function processes user questions. This should be triggered when user has other requirement apart from downloading file.
        If user asks to download and fetch details, this function should be called after downloading file.
        If user asks to fetch details without there is mention of downloading file, this function should be called directly.

        Args:
            ticker: ticker of the company
            document_type: type of document required for this question, either 10K or 10Q or others
            period: period of document required for this question. period should be quarterly notation or fiscal year notation. format examples: 2022Q1, 2022FY
        """
        db = DBFunctions()
        comp_info = db.get_company_info(ticker)
        company_id = comp_info['id']
        document_info = db.get_table_data('documents', filter_dict={
            'companyid': company_id, 'ReportType': document_type.replace('-', ''), 
            'period': period})

        if document_info.empty:
            yield 'Requested data is not yet available. Please try again later.'
        
        document_folder = document_info.iloc[0]['folderlocation']

        minio_fns = MinioDBFunctions('secreports')
        metadata = json.loads(minio_fns.get_object(document_folder+'/metadata.json'))
        html = minio_fns.get_object(metadata['mainHTML'])
        rd_fns.update_status(f"Reading and Understanding the document. for period: {period} and company: {ticker}", self.uid)
        logger.debug(f"Reading and Understanding the document. for period: {period} and company: {ticker}")
        pages = get_pages_text(html)
        relevant_docs = get_relevant_docs(pages, question, metadata['mainHTML'])
        context = '\n'.join(relevant_docs)
        prompt = f"context:{context}, question:{question}"
        rd_fns.update_status("Getting final information from document", self.uid)
        for ans in get_haiku_answer(messages = [{"role": "user", "content": prompt}]):
            yield {
                'answer': ans,
                'sources': [{
                    'name': comp_info['Name'] + ' ' + period + ' ' + document_type,
                    'url': minio_fns.get_full_url(metadata['mainHTML'])
                }]
            }


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

    chain = prompt | haiku_llm | parser
    prompt = f"context: {question}, fill values required to in json"

    query_data = chain.invoke({"query": prompt})
    ticker = query_data['Ticker']
    db_conn = DBFunctions()
    company_info = db_conn.get_company_info(ticker)
    if not company_info:
        return None
    logger.debug(f"company: {company_info['Name']}")
    company_info['docs'] = db_conn.get_table_data(
        "documents", filter_dict={"companyid": company_info['id']})
    return company_info


def get_relevant_docs(pages, question, html_path):
    def hash_list_of_strings(strings):
        sha_signature = hashlib.sha256()
        for string in strings:
            sha_signature.update(string.encode())
        return sha_signature.hexdigest()

    collection_name = re.sub("[^a-zA-Z]", "", html_path)[-20:] + hash_list_of_strings(pages)[:40]
    vectorstore = Chroma(
        collection_name=collection_name,
        embedding_function=HuggingFaceEmbeddings(model_name="WhereIsAI/UAE-Large-V1"),
        # embedding_function=CustomOpenAIEmbeddings(
        #     model="text-embedding-3-large", 
        #     openai_api_key="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"),
        persist_directory=os.path.expanduser("~/Data/vectorcache")
    )
    documents = [Document(page_content=s, metadata={'page':i+1}) for i, s in enumerate(pages)]
    if not vectorstore._collection.count():
        vectorstore.add_documents(documents)

    retriever = vectorstore.as_retriever(search_kwargs={"k": 6})
    relevant_docs = retriever.get_relevant_documents(question)
    relevant_docs = sorted(relevant_docs, key=lambda x: int(x.metadata['page']))
    logger.debug(f"RAG selected pages: {','.join([str(x.metadata['page']) for x in relevant_docs])}")
    relevant_docs = [d.page_content for d in relevant_docs]
    return relevant_docs
