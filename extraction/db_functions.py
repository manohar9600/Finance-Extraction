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
from extraction.html_functions import get_pages_text
from extraction.gpt_functions import summarize, get_openai_embeddingfn
from tqdm import tqdm


class DBFunctions:
    def __init__(self) -> None:
        pass

    @staticmethod
    def _get_connection():
        conn = psycopg2.connect(
            database="ProdDB", host="localhost", user="postgres", password="manu1234", port="5432"
        )
        return conn

    def get_table_data(self, table_name: str, filter_dict={}) -> pd.DataFrame:
        conn = self._get_connection()
        cursor = conn.cursor()
        query = f"select * from {table_name}"
        if filter_dict:
            query += " where"
            for key in filter_dict:
                query += f" {key}={filter_dict[key]}"
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

    def add_document_metadata(self, symbol, publisheddate, filetype, folderlocation):
        companyid = self.get_company_id(symbol)
        conn = self._get_connection()
        cursor = conn.cursor()
        columns = ["companyid", "publisheddate", "filetype", "folderlocation"]
        cursor.execute(
            f"INSERT INTO documents({','.join(columns)}) VALUES({','.join(['%s']*len(columns))})",
            [companyid, publisheddate, filetype, folderlocation],
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

    def get_client(self):
        client = Minio(
            "localhost:9000",
            access_key="1T8p7bcYuD4DgWSRrUGn",
            secret_key="b849fJXr4IjXqATibsiljbrKWA1VrFK4XspD2Hn1",
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


class VectorDBFunctions:
    
    def get_relevant_documents_html(self, html, query):
        documents = get_pages_text(html)
        collection_name = hashlib.sha224(html.encode('utf-8')).hexdigest()
        vectorstore = Chroma(
            collection_name=collection_name,
            embedding_function=get_openai_embeddingfn(),
            persist_directory="vectorcache"
        )
        if vectorstore._collection.count() < 3:
            descriptions = []
            for text in tqdm(documents):
                descriptions.append(summarize(text))
            # adding page number to sort them in context
            documents = [a + "||" + str(i) for i, a in enumerate(documents)]
            summary_texts = [
                Document(page_content=s, metadata={'text': documents[i]})
                for i, s in enumerate(descriptions)
            ]
            vectorstore.add_documents(summary_texts)
        retriever = vectorstore.as_retriever()
        relevant_docs = retriever.get_relevant_documents(query)
        relevant_docs = [d.metadata['text'] for d in relevant_docs]
        relevant_docs = sorted(relevant_docs, key=lambda x: int(x.split("||")[-1]))
        logger.debug(f"RAG selected pages: {','.join([x.split('||')[-1] for x in relevant_docs])}")
        return relevant_docs


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