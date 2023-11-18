# this file fetches data from SEC using company symbol
# search for datapoint in xbrl and inserts into database
import os
import requests
import json
from datetime import date, datetime
from secedgar import CompanyFilings, FilingType
from bs4 import BeautifulSoup
from loguru import logger
from data_insertor import map_datapoint_values, insert_values, get_prod_variables


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
        html_master = "/".join(filing_url.split('/')[:-1])
        index_url = filing_url.replace(".txt", "-index-headers.html")
        folder_path = f"{master_folder}/{cik}/" + html_master.split('/')[-1]

        if os.path.exists(os.path.join(folder_path, "xbrl_pre.json")) and False:
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
        metadata = { "cik": cik, "filingDirectory": html_master, "filingURL": filing_url}
        with open(os.path.join(folder_path, "metadata.json"), 'w') as f:
            json.dump(metadata, f, indent=4)
        
        xbrl_paths = [html_file_path, download_file(get_xbrl_xml_path(
            html_master, index_content), folder_path)[1], 
            download_file(get_extracted_xml_path(
            html_master, filing_url, folder_path), folder_path)[1]]
        match_insert_values(vars_df, xbrl_paths, folder_path)


def match_insert_values(vars_df, xbrl_paths, folder_path):
    xbrl_data = {}
    hierarchy = {}
    for path in xbrl_paths:
        if not path:
            continue
        xbrl_data, hierarchy = get_xbrl_data(path, folder_path)
        if not xbrl_data or not xbrl_data['factList']:
            continue
        results = map_datapoint_values(xbrl_data, vars_df, hierarchy, folder_path)
        if not results:
            continue
            # inserting data into database
            # insert_values(cik, results)
        break
    else:
        logger.warning("got empty xbrl data")


def get_xbrl_data(html_file_path, folder_path):
    server_url = "http://localhost:9000"

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


if __name__ == "__main__":
    vars_df = get_prod_variables()
    master_folder = 'data/current'
    ciks = ["FLT","FMC","F","FTNT","FTV","FOXA","FOX","BEN","FCX","GRMN","IT","GEHC","GEN","GNRC","GD","GE","GIS","GM","GPC","GILD","GL","GPN","GS","HAL","HIG","HAS","HCA","NUE","PEAK","HSIC","HSY","HES","HPE","HLT","HOLX","HD","HON","HRL","HST","HWM","HPQ","HUM","HBAN","HII","IBM","IEX","IDXX","ITW","ILMN","INCY","IR","PODD","INTC","ICE","IFF","IP","IPG","INTU","ISRG","IVZ","INVH","IQV","IRM","JBHT","JKHY","J","JNJ","JCI","JPM","JNPR","K","KVUE","KDP","KEY","KEYS","KMB","KIM","KMI","KLAC","KHC","KR","LHX","LH","LRCX","LW","LVS","LDOS","LEN","LIN","LYV","LKQ","LMT","L","LOW","LYB","MTB","MRO","MPC","MKTX","MAR","MMC","MLM","MAS","MA","MTCH","MKC","MCD","MCK","MDT","MRK","META","MET","MTD","MGM","MCHP","MU","MSFT","MAA","MRNA","MHK","MOH","TAP","MDLZ","MPWR","MNST","MCO","MS","MOS","MSI","MSCI","NDAQ","NTAP","NFLX","NEM","NWSA","NWS","NEE","NKE","NI","NDSN","NSC","NTRS","NOC","NCLH","NRG","NVDA","NVR","NXPI","ORLY","OXY","ODFL","OMC","ON","OKE","ORCL","OGN","OTIS","PCAR","PKG","PANW","PARA","PH","PAYX","PAYC","PYPL","PNR","PEP","PFE","PCG","PM","PSX","PNW","PXD","PNC","POOL","PPG","PPL","PFG","PG","PGR","PLD","PRU","PEG","PTC","PSA","PHM","QRVO","PWR","QCOM","DGX","RL","RJF","RTX","O","REG","REGN","RF","RSG","RMD","RVTY","RHI","ROK","ROL","ROP","ROST","RCL","SPGI","CRM","SBAC","SLB","STX","SEE","SRE","NOW","SHW","SPG","SWKS","SJM","SNA","SEDG","SO","LUV","SWK","SBUX","STT","STLD","STE","SYK","SYF","SNPS","SYY","TMUS","TROW","TTWO","TPR","TRGP","TGT","TEL","TDY","TFX","TER","TSLA","TXN","TXT","TMO","TJX","TSCO","TT","TDG","TRV","TRMB","TFC","TYL","TSN","USB","UDR","ULTA","UNP","UAL","UPS","URI","UNH","UHS","VLO","VTR","VRSN","VRSK","VZ","VRTX","VFC","VTRS","VICI","V","VMC","WAB","WBA","WMT","WBD","WM","WAT","WEC","WFC","WELL","WST","WDC","WRK","WY","WHR","WMB","WTW","GWW","WYNN","XEL","XYL","YUM","ZBRA","ZBH","ZION","ZTS"]
    # ciks = ['A']
    ciks = ['AAPL']
    for cik in ciks:
        logger.info(f"processing company. symbol:{cik}")
        download_company_files(master_folder, cik, vars_df)
    

    # from glob import glob
    # ciks = [os.path.basename(folder) for folder in glob("data/current/*")]
    # ciks = ciks[ciks.index('D'):]
    # for cik in ciks:
    #     logger.info(f"processing company. symbol:{cik}")
    #     download_company_files(master_folder, cik, vars_df)