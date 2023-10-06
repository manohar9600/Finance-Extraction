# this file fetches data from SEC using company symbol
# search for datapoint in xbrl and inserts into database
import os
import requests
import json
from datetime import date, datetime
from secedgar import CompanyFilings, FilingType
from bs4 import BeautifulSoup
from loguru import logger
from data_insertor import map_datapoint_values, insert_values


def get_filing_urls(cik):
    cik = cik.split('.')[0]
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
    return r.text, file_path


def download_company_files(master_folder, cik):
    filing_urls = get_filing_urls(cik)
    for filing_url in filing_urls:
        html_master = "/".join(filing_url.split('/')[:-1])
        index_url = filing_url.replace(".txt", "-index-headers.html")
        folder_path = f"{master_folder}/{cik}/" + html_master.split('/')[-1]
        if os.path.exists(os.path.join(folder_path, "xbrl_data.json")):
            continue
        logger.info(f"processing {filing_url}")
        os.makedirs(folder_path, exist_ok=True)
        index_content, _ = download_file(index_url, folder_path)
        soup = BeautifulSoup(index_content, "html.parser")

        links = soup.find_all("a")
        for link in links:
            if "Financial_Report.xlsx" in link['href'] or ".xml" in link['href'] or \
                    ".xsd" in link['href']:
                download_file(html_master + "/" +  link['href'], folder_path)

        _, html_file_path = download_file(html_master + "/" + links[0]['href'], folder_path)
        metadata = {
            "cik": cik,
            "filingDirectory": html_master,
            "filingURL": filing_url
        }
        
        with open(os.path.join(folder_path, "metadata.json"), 'w') as f:
            json.dump(metadata, f, indent=4)
        
        xbrl_data = get_xbrl_data(html_file_path)
        if not xbrl_data['factList']:
            xbrl_xml_name = get_xbrl_xml_path(index_content)
            if xbrl_xml_name:
                _, html_file_path = download_file(html_master + "/" + xbrl_xml_name, folder_path)
                xbrl_data = get_xbrl_data(html_file_path)

        if not xbrl_data or not xbrl_data['factList']:
            logger.warning("got empty xbrl data")
        else:
            # inserting data into database
            results = map_datapoint_values(xbrl_data)
            insert_values(cik, results)

        with open(os.path.join(folder_path, "xbrl_data.json"), 'w') as f:
            json.dump(xbrl_data, f, indent=4)
        

def get_xbrl_data(html_file_path):
    server_url = "http://localhost:9000"
    html_file_path = os.path.abspath(html_file_path)
    html_file_path = html_file_path.replace("\\", "/")
    req_url = f"{server_url}/rest/xbrl/view?file={html_file_path}&view=facts&media=json&factListCols=Label,unitRef,Dec,Value,Period,Dimensions"
    r = requests.get(req_url)
    return r.json()


def get_xbrl_xml_path(index_content):
    xml_text = BeautifulSoup(index_content, "html.parser").get_text()
    soup = BeautifulSoup(xml_text, 'lxml')

    link_description = ''
    for doc in soup.find_all('document'):
        try:
            doc_description = doc.description.find(string=True, recursive=False)
        except:
            doc_description = ''
        if "xbrl instance document" in doc_description.lower():
            link_description = doc.find('text').get_text().strip('\n')
            break

    xbrl_xml_link = ""
    soup = BeautifulSoup(index_content, "html.parser")
    for link in soup.find_all('a'):
        if link_description in link.get_text():
            xbrl_xml_link = link.get('href')
            break
    return xbrl_xml_link


if __name__ == "__main__":
    master_folder = 'data/current'
    ciks = []
    for cik in ciks:  
        logger.info(f"processing company. symbol:{cik}")
        download_company_files(master_folder, cik)