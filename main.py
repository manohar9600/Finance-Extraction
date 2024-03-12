from extraction.process_sec import *
from extraction.company_info import *
from glob import glob
from extraction.utils import get_latest_file
logger.add("log_file.log")

vars_df = get_prod_variables()
master_folder = 'data/current'
ciks = ["FLT","FMC","F","FTNT","FTV","FOXA","FOX","BEN","FCX","GRMN","IT","GEHC","GEN","GNRC","GD","GE","GIS","GM","GPC","GILD","GL","GPN","GS","HAL","HIG","HAS","HCA","NUE","PEAK","HSIC","HSY","HES","HPE","HLT","HOLX","HD","HON","HRL","HST","HWM","HPQ","HUM","HBAN","HII","IBM","IEX","IDXX","ITW","ILMN","INCY","IR","PODD","INTC","ICE","IFF","IP","IPG","INTU","ISRG","IVZ","INVH","IQV","IRM","JBHT","JKHY","J","JNJ","JCI","JPM","JNPR","K","KVUE","KDP","KEY","KEYS","KMB","KIM","KMI","KLAC","KHC","KR","LHX","LH","LRCX","LW","LVS","LDOS","LEN","LIN","LYV","LKQ","LMT","L","LOW","LYB","MTB","MRO","MPC","MKTX","MAR","MMC","MLM","MAS","MA","MTCH","MKC","MCD","MCK","MDT","MRK","META","MET","MTD","MGM","MCHP","MU","MSFT","MAA","MRNA","MHK","MOH","TAP","MDLZ","MPWR","MNST","MCO","MS","MOS","MSI","MSCI","NDAQ","NTAP","NFLX","NEM","NWSA","NWS","NEE","NKE","NI","NDSN","NSC","NTRS","NOC","NCLH","NRG","NVDA","NVR","NXPI","ORLY","OXY","ODFL","OMC","ON","OKE","ORCL","OGN","OTIS","PCAR","PKG","PANW","PARA","PH","PAYX","PAYC","PYPL","PNR","PEP","PFE","PCG","PM","PSX","PNW","PXD","PNC","POOL","PPG","PPL","PFG","PG","PGR","PLD","PRU","PEG","PTC","PSA","PHM","QRVO","PWR","QCOM","DGX","RL","RJF","RTX","O","REG","REGN","RF","RSG","RMD","RVTY","RHI","ROK","ROL","ROP","ROST","RCL","SPGI","CRM","SBAC","SLB","STX","SEE","SRE","NOW","SHW","SPG","SWKS","SJM","SNA","SEDG","SO","LUV","SWK","SBUX","STT","STLD","STE","SYK","SYF","SNPS","SYY","TMUS","TROW","TTWO","TPR","TRGP","TGT","TEL","TDY","TFX","TER","TSLA","TXN","TXT","TMO","TJX","TSCO","TT","TDG","TRV","TRMB","TFC","TYL","TSN","USB","UDR","ULTA","UNP","UAL","UPS","URI","UNH","UHS","VLO","VTR","VRSN","VRSK","VZ","VRTX","VFC","VTRS","VICI","V","VMC","WAB","WBA","WMT","WBD","WM","WAT","WEC","WFC","WELL","WST","WDC","WRK","WY","WHR","WMB","WTW","GWW","WYNN","XEL","XYL","YUM","ZBRA","ZBH","ZION","ZTS"]
ciks = ['LVS']
# ciks = ['DAL', 'DE', 'DFS', 'DG', 'DHI', 'DHR', 'DIS', 'DLR', 'DOV', 'DOW', 'DPZ', 'DRI', 'DTE', 'DVA', 'DVN', 'DXCM', 'ED', 'FANG', 'GLW', 'KO', 'STZ', 'XRAY', 'ADSK', 'AVB', 'BAC', 'BKNG', 'C', 'CF', 'CINF', 'CTSH', 'DLTR']

# for cik in ciks:
#     logger.info(f"processing company. symbol:{cik}")
#     download_company_files(master_folder, cik, vars_df)

# processing single filing url
# filings = [
#     'https://www.sec.gov/Archives/edgar/data/1551152/000155115222000007/0001551152-22-000007.txt',
#     'abbv'
# ]
# process_filing_url(filings[0], master_folder, filings[1], vars_df)

# # reprocessing a folder
# folder_paths = glob(r"C:\Users\Manohar\Desktop\Projects\Finance-Extraction\data\current\AAPL\*")
# # folder_paths = [r"C:\Users\Manohar\Desktop\Projects\Finance-Extraction\data\current\AAPL\000032019321000105"]
# for folder_path in folder_paths:
#     logger.info(f"processing {folder_path}")
#     cik = os.path.basename(os.path.dirname(folder_path))
#     reprocess_folder(folder_path, vars_df, cik)

# # reprocessing segment information
# company_folders = glob(r"C:\Users\Manohar\Desktop\Projects\Finance-Extraction\data\current\*")
# for company_folder in company_folders:
#     file_path = os.path.join(company_folder, "segment_tables.json")
#     if os.path.exists(file_path):
#         continue
#     logger.info(f"processing {company_folder}")
#     cik = os.path.basename(company_folder)
#     process_segment_information(cik, company_folder)



# processing segment information
company_folders = sorted(list(glob("data/Current/AAL")))
for company in list(company_folders)[:5]:
    logger.info(f"processing company:{company}")
    latest_file = get_latest_file(company)
    seg_info_path = os.path.join(os.path.dirname(
        os.path.dirname(latest_file)), 'segments_info.json')
    # if os.path.exists(seg_info_path):
    #     continue
    segments_info = get_company_segments_v2(latest_file)
    # with open(seg_info_path, 'w', encoding='utf-8') as f:
    #     json.dump(segments_info, f, indent=4, ensure_ascii=False)


# html_path = "data/Current/AAPL/000032019320000096/aapl-20200926.htm"
# print(get_company_segments(html_path))
# os.path.pardir(html_path)
