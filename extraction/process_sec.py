# this file fetches data from SEC using company symbol
# search for datapoint in xbrl and inserts into database
import os
import requests
import json
from datetime import date, datetime
from secedgar import CompanyFilings, FilingType
from bs4 import BeautifulSoup
from loguru import logger
from extraction.data_insertor import DataInsertor, get_prod_variables
from extraction.db_functions import MinioDBFunctions, DBFunctions
from extraction.data_processor import extract_segment_information


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
    def _download(link, file_path):
        headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.63 Safari/537.36"
            }
        r = requests.get(link, headers=headers)
        with open(file_path, 'wb') as f:
            f.write(r.content)
        file_content = r.text
        return file_content
    if not link:
        return '', '' 
    file_name = link.split('/')[-1]
    file_path = os.path.join(folder_path, file_name)

    if os.path.exists(file_path):
        try:
            with open(file_path, 'r') as f:
                file_content = f.read()
        except:
            file_content = _download(link, file_path)
    else:
        file_content = _download(link, file_path)
    return file_content, file_path


def download_company_files(master_folder, cik, vars_df):
    filing_urls = get_filing_urls(cik)
    for filing_url in filing_urls:
        process_filing_url(filing_url, master_folder, cik, vars_df)


def process_filing_url(filing_url, master_folder, cik, vars_df):
    html_master = "/".join(filing_url.split('/')[:-1])
    index_url = filing_url.replace(".txt", "-index-headers.html")
    folder_path = f"{master_folder}/{cik}/" + html_master.split('/')[-1]

    if os.path.exists(os.path.join(folder_path, "xbrl_pre.json")) and False:
        return
    
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
    metadata = { "cik": cik, "filingDirectory": html_master, "filingURL": filing_url}
    with open(os.path.join(folder_path, "metadata.json"), 'w') as f:
        json.dump(metadata, f, indent=4)
    
    xbrl_paths = [html_file_path, download_file(get_xbrl_xml_path(
        html_master, index_content), folder_path)[1], 
        download_file(get_extracted_xml_path(
        html_master, filing_url, folder_path), folder_path)[1]]
    match_insert_values(vars_df, xbrl_paths, folder_path, cik)
    # fetch date from results
    # use date to store in metadata in db and in minio


def match_insert_values(vars_df, xbrl_paths, folder_path, cik):
    xbrl_data = {}
    hierarchy = {}
    for path in xbrl_paths:
        if not path:
            continue
        xbrl_data, hierarchy = get_xbrl_data(path, folder_path)
        if not xbrl_data or not xbrl_data['factList']:
            continue
        data_insertor = DataInsertor(cik, xbrl_data, hierarchy)
        results = data_insertor.map_datapoint_values(vars_df, folder_path)
        if not results:
            continue

        # inserting data into database
        # data_insertor.insert_values(results)

        # uploading files to bucket
        stored_folder = MinioDBFunctions("secreports").upload_folder(folder_path)
        DBFunctions().add_document_metadata(cik, data_insertor.document_period, '10K', stored_folder)
        break
    else:
        raise ValueError('failed to get xbrl data')
    return data_insertor
    

def reprocess_folder(folder_path, vars_df, cik):
    # function to calculate values without download and without fetching xbrl data
    xbrl_data, hierarchy = get_xbrl_data('', folder_path)
    if not xbrl_data or not xbrl_data['factList']:
        return
    data_insertor = DataInsertor(cik, xbrl_data, hierarchy)
    results = data_insertor.map_datapoint_values(vars_df, folder_path)

    # # uploading files to bucket
    # stored_folder = MinioDBFunctions("secreports").upload_folder(cik, folder_path)
    # DBFunctions().add_document_metadata(cik, data_insertor.document_period, '10K', stored_folder)

    # inserting data into database
    # data_insertor.insert_values(results)


def get_xbrl_data(html_file_path, folder_path):
    server_url = "http://localhost:8080"

    if os.path.exists(os.path.join(folder_path, "xbrl_data.json")):
        with open(os.path.join(folder_path, "xbrl_data.json"), 'r') as f:
            xbrl_data = json.load(f)
    else:
        html_file_path = os.path.abspath(html_file_path)
        html_file_path = html_file_path.replace("\\", "/")
        req_url = f"{server_url}/rest/xbrl/view?file={html_file_path}&view=facts&media=json&factListCols=Label,unitRef,Dec,Value,Period,Dimensions"
        r = requests.get(req_url)
        xbrl_data = r.json()
        with open(os.path.join(folder_path, "xbrl_data.json"), 'w') as f:
            json.dump(xbrl_data, f, indent=4)
    
    if os.path.exists(os.path.join(folder_path, "xbrl_pre.json")):
        with open(os.path.join(folder_path, "xbrl_pre.json"), 'r') as f:
            hierarchy = json.load(f)
    else:
        req_url = f"{server_url}/rest/xbrl/view?file={html_file_path}&view=pre&media=json"
        r = requests.get(req_url)
        hierarchy = r.json()
        with open(os.path.join(folder_path, "xbrl_pre.json"), 'w') as f:
            json.dump(hierarchy, f, indent=4)

    return xbrl_data, hierarchy


def get_xbrl_xml_path(html_master, index_content):
    xml_text = BeautifulSoup(index_content, "html.parser").get_text()
    soup = BeautifulSoup(xml_text, 'lxml')

    link_description = ''
    for doc in soup.find_all('document'):
        try:
            doc_type = doc.type.find(string=True, recursive=False)
        except:
            doc_type = ''
        if "EX-101.INS" in doc_type:
            link_description = doc.find('text').get_text().strip('\n')
            break

    xbrl_xml_link = ""
    soup = BeautifulSoup(index_content, "html.parser")
    for link in soup.find_all('a'):
        if link_description and link_description in link.get_text():
            xbrl_xml_link = link.get('href')
            break
    if xbrl_xml_link:
        return html_master + "/" + xbrl_xml_link.strip("/")
    return xbrl_xml_link


def get_extracted_xml_path(html_master, filing_url, folder_path):
    index_html_url = filing_url.replace(".txt", "-index.html")
    index_html_content, _ = download_file(index_html_url, folder_path)
    soup = BeautifulSoup(index_html_content, "html.parser")
    link = soup.find_all('table')[-1].find_all('tr')[-1].find_all('td')[2].find('a')['href']
    return "https://www.sec.gov" + link


def process_segment_information(symbol, folder_path):
    extract_segment_information(symbol, folder_path)
