# --- download agent to fetch files for public sec companies.
import os
import sys
import requests
from datetime import date, datetime
from secedgar import CompanyFilings, FilingType
from loguru import logger
import json
from dateutil.relativedelta import relativedelta
import pandas as pd
from bs4 import BeautifulSoup
from enlighten import get_manager
from xbrl.cache import HttpCache
from xbrl.instance import XbrlParser
from xml.etree.ElementTree import ParseError
from dateutil.parser import parse as dateparse

current_script_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_script_dir)
sys.path.append(parent_dir)
from extraction.db_functions import DBFunctions, MinioDBFunctions
from extraction.utils import *

pbar_manager = get_manager()
db_fns = DBFunctions()
minio_fns = MinioDBFunctions("secreports")


def get_company_filing_urls(cik):
    company_id = db_fns.get_company_id(cik)
    master_start_date = date(2019, 1, 1)
    master_end_date = datetime.now().date()
    filing_types = [
        (FilingType.FILING_10K, relativedelta(months=12)),
        (FilingType.FILING_10Q, relativedelta(months=3)),
    ]  # filing time and time period
    filings = []
    for filing_type, time_period in filing_types:
        docs_pres = db_fns.get_table_data(
            "documents",
            filter_dict={
                "companyid": company_id,
                "ReportType": filing_type.value.replace("-", ""),
            },
        )

        # code to fetch missing periods in the filings
        if pd.isna(docs_pres["publisheddate"].min()) or (
            docs_pres["publisheddate"].min() - time_period > master_start_date
        ):
            start_date = master_start_date
        else:
            start_date = docs_pres["publisheddate"].max() + relativedelta(days=1)
        if pd.isna(docs_pres["publisheddate"].min()) or (
            docs_pres["publisheddate"].max() + time_period < master_end_date
        ):
            end_date = master_end_date
        else:
            end_date = docs_pres["publisheddate"].min() - relativedelta(days=1)
        if start_date >= end_date:
            continue

        # code to fetch missing filings
        filing = CompanyFilings(
            cik_lookup=cik,
            filing_type=filing_type,
            start_date=start_date,
            end_date=end_date,
            user_agent="Name (email)",
        )
        processed_ids = [os.path.basename(_f) for _f in docs_pres["folderlocation"]]
        current_filings = filing.get_urls()
        for _f in current_filings[cik]:
            if _f.split("/")[-2] not in processed_ids:
                filings.append(_f)
    return filings


def search_xbrl_data(xbrl_data, search_key):
    for key in xbrl_data["facts"]:
        fact = xbrl_data["facts"][key]
        if fact["dimensions"]["concept"] == search_key:
            return fact["value"]


def process_sec_files(cik):
    filing_urls = get_company_filing_urls(cik)
    fiscal_year = db_fns.get_company_info(cik)["Fiscal Year"]
    pbar = pbar_manager.counter(total=len(filing_urls), desc="Processing Filings:", leave=False)
    for filing_url in filing_urls:
        logger.info(f"processing {filing_url}")
        xbrl_data, folder_path = process_sec_filing(filing_url, cik)
        if not xbrl_data or not xbrl_data.get("facts", []):
            logger.info('got empty xbrl data')
            continue
        document_period = search_xbrl_data(xbrl_data, "DocumentPeriodEndDate")
        doc_type = search_xbrl_data(xbrl_data, "DocumentType").replace("-", "")
        # doc_period = datetime.strptime(document_period, "%Y-%m-%d") 
        doc_period = dateparse(document_period)
        if doc_type == "10K":
            period = str(doc_period.year) + "FY"
        elif doc_type == "10Q":
            months = month_difference(doc_period, datetime.strptime(fiscal_year, "%m-%d")) % 12
            notation_yr = doc_period.year
            # eg: quarter in middle of 2023-24 -> 2024Q2
            if datetime.strptime(str(doc_period.year)+"-"+fiscal_year, "%Y-%m-%d") < doc_period:
                notation_yr += 1
            period = str(notation_yr) + "Q" + str(months // 3)
        if not db_fns.is_doc_instance_exists(
            cik, document_period, doc_type, folder_path, period
        ):
            db_fns.add_document_metadata(
                cik, document_period, doc_type, folder_path, period
            )
        logger.info(f"processed {filing_url}")
        pbar.update(1)
    pbar.close()


def download_file(link, folder_path):
    def _download(link, file_path):
        logger.info(f"downloading file, {link}")
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.63 Safari/537.36"
        }
        r = requests.get(link, headers=headers)
        file_content = r.text
        minio_fns.put_object(file_path, file_content)
        return file_content

    if not link:
        return "", ""

    file_name = link.split("/")[-1]
    file_path = folder_path + '/' + file_name

    if minio_fns.is_file_present(file_path):
        file_content = minio_fns.get_object(file_path)
    else:
        file_content = _download(link, file_path)
    return file_content, file_path


def get_xbrl_data(folder_path, links, html_file_path):
    xbrl_path = folder_path + '/' + "xbrl_data.json"
    if minio_fns.is_file_present(xbrl_path):
        xbrl_json = minio_fns.get_object(xbrl_path)
    else:
        logger.info('generating xbrl')
        cache: HttpCache = HttpCache(os.path.expanduser('~/.cache/xbrl_cache/'))
        parser = XbrlParser(cache)
        try:
            xbrl_instance = parser.parse_instance("http://" + minio_fns.url +
                                                  '/secreports/' + html_file_path)
        except ParseError as e:
            xml_link = ''
            for link in links:
                if link["href"].endswith(".xml"):
                    xml_link = link["href"]
                    break
            xbrl_instance = parser.parse_instance("http://"+ minio_fns.url + '/secreports/' + 
                                                  folder_path + '/'+ xml_link)
        xbrl_json = xbrl_instance.json(override_fact_ids=True)
        minio_fns.put_object(xbrl_path, xbrl_json)
    xbrl_json = json.loads(xbrl_json)
    return xbrl_json


def process_sec_filing(filing_url, cik):
    html_master = "/".join(filing_url.split("/")[:-1])
    index_url = filing_url.replace(".txt", "-index-headers.html")
    folder_path = f"{cik}/{html_master.split('/')[-1]}"

    index_content, _ = download_file(index_url, folder_path)
    soup = BeautifulSoup(index_content, "html.parser")
    links = soup.find_all("a")
    pbar = pbar_manager.counter(total=len(links), desc="Downloading Files:", leave=False)
    for link in links:
        download_file(html_master + "/" + link["href"], folder_path)
        pbar.update(1)
    pbar.close()

    html_file_path = folder_path + '/' + links[0]["href"].split("/")[-1]
    metadata = {
        "cik": cik,
        "filingDirectory": html_master,
                "filingURL": filing_url, "mainHTML":html_file_path}
    minio_fns.put_object(f"{folder_path}/metadata.json", json.dumps(metadata))

    # parsing xbrl data
    xbrl_json = get_xbrl_data(folder_path, links, html_file_path)
    return xbrl_json, folder_path


if __name__ == '__main__':
    # process_sec_files('MSFT')

    # -- full run --
    tickers = db_fns.get_table_data('companies')['Symbol'].to_list()
    pbar = pbar_manager.counter(total=len(tickers), desc="Companies:", leave=False)
    for tic in tickers:
        try:
            process_sec_files(tic)
        except:
            pass
        pbar.update(1)
    pbar.close()
