import json
import os
import warnings
from datetime import date, datetime

warnings.filterwarnings("ignore")

import requests
from loguru import logger
from secedgar import CompanyFilings, FilingType

from tables import *


def get_filing_urls(cik):
    filing = CompanyFilings(cik_lookup=cik,
                            filing_type=FilingType.FILING_10K,
                            start_date=date(2018, 1, 1),
                            end_date=datetime.now(),
                            user_agent="Name (email)")

    company_filings_urls = filing.get_urls()
    return company_filings_urls[cik]


def download_company_files(cik):
    headers = {
       "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.63 Safari/537.36"
    }
    os.makedirs(f'data/{cik}', exist_ok=True)
    filing_urls = get_filing_urls(cik)
    for filing_url in filing_urls:
        file_name = filing_url.split('/')[-1].replace('.txt', '')
        file_path = f'data/{cik}/{file_name}.json'
        if os.path.exists(file_path):
            continue
        logger.info(f"processing {filing_url}")
        r = requests.get(filing_url, headers=headers)
        if r.status_code == 200:
            tables = get_html_tables(r.text)
            tables = classify_tables(tables)
            tables = construct_proper_tables(tables)
            file_name = filing_url.split('/')[-1].replace('.txt', '')
            file_path = f'data/{cik}/{file_name}.json'
            data = {
                'url': filing_url,
                'tables': tables
            }
            with open(file_path, 'w') as f:
                json.dump(data, f, indent=4)
            with open(file_path.replace(".json", ".html"), 'w') as f:
                f.write(r.text)


if __name__ == "__main__":
    ciks = ['tsla']
    for cik in ciks:
        logger.info(f"processing company, cik:{cik}")
        download_company_files(cik)
        
