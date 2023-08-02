import openpyxl
import xml.etree.ElementTree as ET

def extract_footnotes(file_path):
    workbook = openpyxl.load_workbook(file_path)
    sheet_names = workbook.sheetnames

    for sheet_name in sheet_names:
        worksheet = workbook[sheet_name]
        rels = worksheet.relationships

        for _, rel in rels.items():
            if "comments" in rel.target_ref:
                # Extract the footnote information from comments
                comments_xml = rel.target_part.blob
                tree = ET.ElementTree(ET.fromstring(comments_xml))
                root = tree.getroot()

                for comment in root.findall(".//comment"):
                    author = comment.find("author").text
                    ref = comment.find("ref").attrib["r"]
                    text = comment.find("text").find("t").text

                    print(f"Sheet '{sheet_name}', Cell {ref}, Author: {author}, Footnote: {text}")

# Usage example
file_path = r"data\AAL\000000620120000023\Financial_Report.xlsx"
extract_footnotes(file_path)
