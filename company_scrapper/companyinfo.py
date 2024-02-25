import os
import json
import time
import requests

save_folder = "company_scrapper\companydata"

url = "https://yahoo-finance15.p.rapidapi.com/api/v1/markets/stock/modules"


def get_company_profile(ticker):

    querystring = {"ticker": ticker, "module": "asset-profile"}

    headers = {
        "X-RapidAPI-Key": "d2d6a4f04dmshf1b53f6544e65a5p1067b4jsn42483a4a4ead",
        "X-RapidAPI-Host": "yahoo-finance15.p.rapidapi.com",
    }

    print(f"starting ticker: {ticker}")
    response = requests.get(url, headers=headers, params=querystring)

    # print(response.json())

    with open(os.path.join(save_folder, f"{ticker}.json"), "w") as f:
        json.dump(response.json(), f, indent=3)

    print(f"save data ticker: {ticker}")
    print("------" * 10)


def get_ticker_data(ticker):
    data_path = os.path.join(save_folder, f"{ticker}.json")

    try:
        with open(data_path, "r") as f:
            data = json.load(f)
    except Exception:
        data = dict()
        return data

    out_dict = dict()

    out_dict["symbol"] = data["meta"]["symbol"]

    for key, value in data["body"].items():

        if key in [
            "auditRisk",
            "boardRisk",
            "compensationRisk",
            "shareHolderRightsRisk",
            "overallRisk",
            "governanceEpochDate",
            "compensationAsOfEpochDate",
            "maxAge",
        ]:
            continue
        
        elif key == 'companyOfficers':
            ceo_data = dict()
            for ofcr in value:
                if 'ceo' in ofcr['title'].lower():
                    ceo_data['name'] = ofcr['name'].lstrip('Mr.').lstrip('Ms.').strip()
                    ceo_data['age'] = ofcr['age']
                    ceo_data['pay'] = ofcr['totalPay']['fmt']
                    
                    out_dict['ceo'] = ceo_data
        else:
            out_dict[key] = value
            
    return out_dict


if __name__ == "__main__":

    out_data = get_ticker_data('C')
    
    companies = [
        ("AAPL",),
        ("AAL",),
        ("ABBV",),
        ("ABNB",),
        ("ABT",),
        ("DLTR",),
    ]

    saved_files = os.listdir(save_folder)

    for i, ctick in enumerate(companies):
        if i == 0:
            continue
        if ctick[0] + ".json" not in saved_files:
            print(f"{i}/{len(companies)}")
            get_company_profile(ctick[0])
            time.sleep(5)
