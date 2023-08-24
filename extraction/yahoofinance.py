import os
import yfinance as yf
import pandas as pd


def save_company_excels(ticker, excel_path):
    comp_info = yf.Ticker(ticker)
    income_statement = comp_info.income_stmt
    balance_sheet = comp_info.balance_sheet
    cash_flow = comp_info.cash_flow

    with pd.ExcelWriter(excel_path) as excel_writer:
        income_statement[::-1].to_excel(excel_writer, sheet_name='income statement')
        balance_sheet[::-1].to_excel(excel_writer, sheet_name='balance sheet')
        cash_flow[::-1].to_excel(excel_writer, sheet_name='cash flow')

    print("DataFrames saved to Excel successfully.")

if __name__ == "__main__":
    target_folder = "data/yahoofinance"
    from glob import glob
    for _f in glob("data/current/*"):
        cik = os.path.basename(_f)
        print(f"processing cik: {cik}")
        target_path = os.path.join(target_folder, cik)
        os.makedirs(target_path, exist_ok=True)
        target_path = os.path.join(target_path, "tables.xlsx")
        save_company_excels(cik, target_path)