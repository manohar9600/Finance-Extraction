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


def download_file(link, folder_path):
    headers = {
       "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.63 Safari/537.36"
    }
    r = requests.get(link, headers=headers)
    file_name = link.split('/')[-1]
    file_path = os.path.join(folder_path, file_name)
    with open(file_path, 'wb') as f:
        f.write(r.content)
    return r.text


def download_company_files(cik):
    filing_urls = get_filing_urls(cik)
    for filing_url in filing_urls:
        html_master = "/".join(filing_url.split('/')[:-1])
        index_url = filing_url.replace(".txt", "-index-headers.html")
        folder_path = f"data/{cik}/" + html_master.split('/')[-1]
        table_json_path = os.path.join(folder_path, "tables.json")
        if os.path.exists(table_json_path):
            continue
        
        logger.info(f"processing {filing_url}")
        os.makedirs(folder_path, exist_ok=True)
        index_content = download_file(index_url, folder_path)
        soup = BeautifulSoup(index_content, "html.parser")
        links = soup.find_all("a")
        for link in links:
            if "Financial_Report.xlsx" in link['href']:
                download_file(html_master + "/" + "Financial_Report.xlsx", folder_path)

        report_html = download_file(html_master + "/" + links[0]['href'], folder_path)
        tables = get_html_tables(report_html)
        tables = classify_tables(tables)
        tables = fix_table_classification(tables)
        tables = construct_proper_tables(tables)

        data = {
            'url': filing_url,
            'tables': tables
        }
        with open(table_json_path, 'w') as f:
            json.dump(data, f, indent=4)

if __name__ == "__main__":
    ciks = ['adbe', 'adb', 'aap', 'aes', 'afl', 'a', 'apd', 'akam', 'alk', 'alb']
    for cik in ciks:
        logger.info(f"processing company, cik:{cik}")
        download_company_files(cik)
        
