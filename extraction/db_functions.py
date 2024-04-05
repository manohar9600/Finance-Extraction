import os
import json
import mimetypes
import psycopg2
import hashlib
import pandas as pd
from glob import glob
from minio import Minio
from loguru import logger
from langchain_community.vectorstores import Chroma
from langchain.docstore.document import Document
from langchain_community.embeddings import HuggingFaceEmbeddings
from langchain_openai import OpenAIEmbeddings
from extraction.html_functions import get_pages_text
from extraction.gpt_functions import summarize
from tqdm import tqdm
import io
from minio.error import S3Error
import redis


class DBFunctions:
    def __init__(self) -> None:
        pass

    @staticmethod
    def _get_connection():
        conn = psycopg2.connect(
            database="ProdDB", host="localhost", user="postgres", password="manu@960", port="5432"
        )
        return conn

    def get_table_data(self, table_name: str, filter_dict={}) -> pd.DataFrame:
        conn = self._get_connection()
        cursor = conn.cursor()
        query = f"select * from {table_name}"
        where_conditions = []
        if filter_dict:
            for key in filter_dict:
                where_conditions.append(f" \"{key}\"='{filter_dict[key]}'")
        if where_conditions:
            query += " where " + " and ".join(where_conditions)
        cursor.execute(query)
        df = pd.DataFrame(cursor, columns=[desc[0] for desc in cursor.description])
        conn.close()
        return df

    def get_company_id(self, symbol: str):
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute(f"SELECT id FROM companies where \"Symbol\"='{symbol.upper()}'")
        comp_id = cursor.fetchone()[0]
        conn.close()
        return comp_id
    
    def get_company_info(self, symbol: str) -> dict:
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute(f"SELECT * FROM companies where \"Symbol\"='{symbol.upper()}'")
        response_data = cursor.fetchone()
        if response_data is None:
            return {}
        columns = [desc[0] for desc in cursor.description]
        company_data = response_data
        output = {}
        for i, col in enumerate(columns):
            output[col] = company_data[i]
        conn.close()
        return output
    
    def is_doc_instance_exists(self, symbol, publisheddate, report_type, folderlocation, period):
        companyid = self.get_company_id(symbol)
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute(
            f"SELECT * FROM documents where companyid={companyid} and publisheddate='{publisheddate}' and \"ReportType\"='{report_type}' and folderlocation='{folderlocation}' and period='{period}'"
        )
        response_data = cursor.fetchone()
        conn.close()
        return response_data is not None

    def add_document_metadata(self, symbol, publisheddate, report_type, folderlocation, period):
        companyid = self.get_company_id(symbol)
        conn = self._get_connection()
        cursor = conn.cursor()
        columns = ["companyid", "publisheddate", "\"ReportType\"", "folderlocation", "period"]
        cursor.execute(
            f"INSERT INTO documents({','.join(columns)}) VALUES({','.join(['%s']*len(columns))})",
            [companyid, publisheddate, report_type, folderlocation, period],
        )
        conn.commit()
        conn.close()
        logger.info(f"updated files metadata. folder loc: {folderlocation}")

    def update_query(self, query):
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute(query)
        conn.commit()
        conn.close()
        logger.info(f"query run completed")


class MinioDBFunctions:
    def __init__(self, bucket_name) -> None:
        self.bucket_name = bucket_name
        self.url = "localhost:9000"

    def get_client(self):
        client = Minio(
            self.url,
            access_key="ARwMHgQXQeGFaCojdTG0",
            secret_key="4rkrfMH2Vba52VkP7afhdCvo3FuOU0cAv3YQL5Ey",
            secure=False,  # Set to False if you are not using https
        )

        return client

    def upload_file_to_minio(self, object_name, file_path):
        client = self.get_client()
        client.fput_object(
            self.bucket_name,
            object_name,
            file_path,
            content_type=mimetypes.guess_type(file_path)[0],
        )

    def upload_folder(self, cik, folder_path):
        target_folder = os.path.join(cik, os.path.basename(folder_path))
        for file_path in glob(os.path.join(folder_path, "*")):
            object_name = os.path.join(target_folder, os.path.basename(file_path))
            object_name = object_name.replace(os.path.sep, "/")
            self.upload_file_to_minio(object_name, file_path)
        logger.info(f"uploaded files to bucket. data folder:{target_folder}")
        return target_folder

    def list_objects(self, folder_name):
        folder_name = folder_name.replace(os.path.sep, "/")
        client = self.get_client()
        objects = client.list_objects(self.bucket_name, prefix=folder_name, recursive=True)
        objects = [obj.object_name for obj in objects]
        return objects
    
    def get_object(self, object_name):
        client = self.get_client()
        data = client.get_object(self.bucket_name, object_name)
        return data.read()
    
    def put_object(self, object_name, data):
        client = self.get_client()
        client.put_object(
            self.bucket_name,
            object_name,
            io.BytesIO(data.encode('utf-8')),
            length=len(data),
            content_type=mimetypes.guess_type(object_name)[0]
        )
        logger.info(f"object saved to bucket. object_name:{object_name}")
    
    def is_file_present(self, file_path):
        client = self.get_client()
        try:
            # Attempt to get the file metadata
            client.stat_object(self.bucket_name, file_path)
            return True
        except S3Error as exc:
            if exc.code == "NoSuchKey":
                return False


class VectorDBFunctions:
    
    def get_relevant_documents_html(self, html, query):
        documents = get_pages_text(html)
        collection_name = hashlib.sha224(html.encode('utf-8')).hexdigest()
        vectorstore = Chroma(
            collection_name=collection_name,
            # embedding_function=HuggingFaceEmbeddings(model_name="WhereIsAI/UAE-Large-V1"),
            embedddding_function=OpenAIEmbeddings(model="text-embedding-3-large"),
            persist_directory="/home/manu/Data/vectorcache"
        )
        if vectorstore._collection.count() < 3:
            descriptions = []
            for text in tqdm(documents, desc="summarizing:"):
                descriptions.append(summarize(text))
            # adding page number to sort them in context
            documents = [a + "||" + str(i) for i, a in enumerate(documents)]
            summary_texts = [
                Document(page_content=s, metadata={'text': documents[i]})
                for i, s in enumerate(descriptions)
            ]
            vectorstore.add_documents(summary_texts)

        retriever = vectorstore.as_retriever(search_kwargs={"k": 10})
        relevant_docs = retriever.get_relevant_documents(query)
        relevant_docs = [d.metadata['text'] for d in relevant_docs]
        relevant_docs = sorted(relevant_docs, key=lambda x: int(x.split("||")[-1]))
        logger.debug(f"RAG selected pages: {','.join([x.split('||')[-1] for x in relevant_docs])}")
        return relevant_docs


class RedisFunctions:

    def __init__(self) -> None:
        pass

    def start_status(self, steps, uid):
        if uid is None:
            return
        status_json = {
            'steps': steps,
            'completed': -1,
            'status': 'Process Intialised'
        }
        self.put_data(status_json, uid)

    def update_status(self, status, uid):
        if uid is None:
            return
        status_json = self.get_data(uid)
        status_json['completed'] += 1
        status_json['status'] = status
        self.put_data(status_json, uid)

    def put_data(self, data, uid):
        r = redis.Redis(host='localhost', port=6379, decode_responses=True)
        r.set(uid, json.dumps(data))
        
    def get_data(self, uid):
        r = redis.Redis(host='localhost', port=6379, decode_responses=True)
        status_json = json.loads(r.get(uid))
        return status_json


def get_segment_data(symbol):
    file_path = os.path.join(
        r"/home/manu/Projects/Finance-Extraction/data/Current",
        symbol,
        "segment_tables.json",
    )
    data = {}
    if os.path.exists(file_path):
        with open(file_path, 'r') as f:
            data = json.load(f)
    return data
    