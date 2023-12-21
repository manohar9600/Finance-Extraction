import re
import os
import json
import pandas as pd
from glob import glob
from extraction.data_insertor import DataInsertor
from extraction.utils import convert_to_number

# segment tables extraction

ref_axis = ["GeographicalAxis", "ProductOrServiceAxis", "BusinessSegmentsAxis"]


def get_segment_lineitems(data):
    segment_items = []
    for fact in data["factList"]:
        axis_list = list(fact[2].get("dimensions", {}).keys())
        for axis in ref_axis:
            for reported_axis in axis_list:
                member = fact[2]["dimensions"][reported_axis]
                if (
                    axis in reported_axis
                    # and "us-gaap:ProductMember" != member
                    # and "us-gaap:ServiceMember" != member
                ):
                    segment_items.append(fact)
                    break
            else:
                continue
            break
    return segment_items


def extract_revenue_information(results, segment_items, doc_period):
    revenue_xbrls = [
        "us-gaap:Revenues",
        "us-gaap:RevenueFromContractWithCustomerExcludingAssessedTax",
        "us-gaap:SalesRevenueNet",
        "us-gaap:RevenueFromContractWithCustomerIncludingAssessedTax",
    ]

    for fact in segment_items:
        if fact[2]["label"] not in revenue_xbrls or fact[2]["endInstant"] != doc_period:
            continue
        axis_list = list(fact[2]["dimensions"].keys())
        if len(axis_list) >= 2:
            continue
        for axis in ref_axis:
            for reported_axis in axis_list:
                if axis not in reported_axis:
                    continue
                member = fact[2]["dimensions"][reported_axis]
                segment = member.split(":")[-1].replace("Member", "").replace("Segment", "")
                segment = " ".join(re.findall("[A-Z][^A-Z]*", segment))

                axis_name = " ".join(
                    re.findall(
                        "[A-Z][^A-Z]*", reported_axis.replace("Axis", "").replace("Statement", "")
                    )
                )
                if axis_name not in results:
                    results[axis_name] = {}
                if segment not in results[axis_name]:
                    results[axis_name][segment] = {}
                results[axis_name][segment][doc_period] = fact[2]["value"]
                break
            else:
                continue
            break
    return results


def extract_segment_information(symbol, parent_folder):
    results = {}
    folder_paths = glob(os.path.join(parent_folder, '*'))
    for folder_path in folder_paths:
        if not os.path.isdir(folder_path):
            continue
        with open(os.path.join(folder_path, "xbrl_data.json")) as f:
            xbrl_data = json.load(f)
        with open(os.path.join(folder_path, "xbrl_pre.json")) as f:
            hierarchy = json.load(f)
        if not xbrl_data or not xbrl_data["factList"]:
            continue
        doc_period = DataInsertor(symbol, xbrl_data, hierarchy).document_period
        segment_items = get_segment_lineitems(xbrl_data)
        results = extract_revenue_information(results, segment_items, doc_period)

    segment_tables = []
    for key in results:
        df = pd.DataFrame(results[key]).T.reset_index()
        df.fillna("", inplace=True)
        body = []
        for _, row in df.iterrows():
            _r = []
            for j, val in enumerate(row):
                if j != 0:
                    val = convert_to_number(val)
                _r.append({'value': val})
            body.append(_r)

        table = {"class": key, "header": [""]+list(df.columns)[1:], "body": body}
        segment_tables.append(table)
    
    file_path = os.path.join(parent_folder, "segment_tables.json")
    with open(file_path, "w") as f:
        json.dump(segment_tables, f, indent=4)
