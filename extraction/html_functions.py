import json
import re
from intervaltree import IntervalTree, Interval
import bs4
from bs4 import BeautifulSoup


def convert_intervaltree(table_tree):
    # function to convert interval tree data to list data
    num_rows = max([len(interval.data) for interval in sorted(table_tree)], default=0)
    table_tree_unwrapped = [[] for _ in range(num_rows)]
    for interval in sorted(table_tree):
        for i, cell in enumerate(interval.data):
            table_tree_unwrapped[i].append(cell)

    table_body = []
    for row in table_tree_unwrapped:
        table_row = []
        for i, cells in enumerate(row):
            if cells is not None and len(cells) > 0:
                cell_dict = {
                    "text": " ".join([_c.get_text(separator=' ').strip() for _c in cells]),
                    "href": cells[-1].find("a").get("href") if cells[-1].find("a") else None,
                    "cells": cells
                }
                table_row.append(cell_dict)
            else:
                table_row.append({"text": ""})
                continue
        table_body.append(table_row)
    return table_body


def divide_column_rows(table_rows):
    column_index = 0
    multi_column_text = ""
    for row in table_rows:
        non_variable_text = ""
        for _c in row.find_all("td")[1:]:
            if _c.find_all("ix:nonfraction"):
                break
            non_variable_text += _c.get_text()
        else:
            if multi_column_text and not non_variable_text.strip() and row.get_text().strip():
                break
            col_texts = [t.get_text().strip() for t in row.find_all("td") if t.get_text().strip()]
            if len(col_texts) > 1:
                multi_column_text = multi_column_text + " ".join(col_texts)
            column_index += 1
            continue
        break

    return table_rows[:column_index], table_rows[column_index:]


def get_text_above_table(table_tag):
    text_above_list = []
    while len(text_above_list) < 5:
        while table_tag is not None and table_tag.previous_sibling is None:
            table_tag = table_tag.parent
        table_tag = table_tag.previous_sibling
        if table_tag is None or table_tag.find_all("table"):
            break
        if table_tag.get_text().strip():
            text_above_list.append(table_tag.get_text().strip())
    text_above = "\n".join(text_above_list[::-1])
    return text_above


def extract_html_table(table_tag):
    table_tree = IntervalTree()
    num_rows = 0
    for row in table_tag.find_all("tr"):
        prev_coord = 0
        table_cells = row.find_all("td")
        for cell in table_cells:
            # len(table_cells) == 1 condition for centered variables.
            if not cell.get("colspan") or len(table_cells) == 1:
                end_coord = prev_coord + 1
            else:
                end_coord = prev_coord + int(cell.get("colspan"))

            if len(cell.get_text().strip()) == 0:
                prev_coord = end_coord
                continue

            overlap_intervals = table_tree.overlap(prev_coord, end_coord)
            if not overlap_intervals:
                table_tree.add(Interval(prev_coord, end_coord, [None] * num_rows + [[cell]]))
            else:
                interval = sorted(overlap_intervals)[0]
                if len(interval.data) > num_rows:
                    interval.data[-1].append(cell)
                else:
                    interval.data.append([cell])
            prev_coord = end_coord

        # equating length of all columns
        num_rows = max([len(interval.data) for interval in sorted(table_tree)], default=0)
        for interval in table_tree:
            for _ in range(len(interval.data), num_rows):
                interval.data.append(None)

    table_dict = {
        "textAbove": get_text_above_table(table_tag),
        "header": [],
        "body": convert_intervaltree(table_tree),
    }
    return table_dict


def extract_html_tables(html_path, limit=None):
    with open(html_path, "r") as f:
        soup = BeautifulSoup(f, "html.parser")

    tables = []
    table_tags = soup.find_all("table")
    if limit is not None:
        table_tags = table_tags[:limit]
    for table_html in table_tags[3:]:
        table = extract_html_table(table_html)
        if table["body"]:
            tables.append(table)

    return tables


def get_glossary(html_path):
    tables = extract_html_tables(html_path, limit=15)
    glossary_table = None
    for table in tables:
        if (
            "glossary" in table["textAbove"].lower()
            or "table of contents" in table["textAbove"].lower()
        ):
            glossary_table = table
            break
    if glossary_table is None:
        return {}
    glossary = {}
    for row in glossary_table["body"]:
        if len(row) == 3 and row[1]["text"]:
            href = ""
            for _c in row:
                if _c.get("href", "") and _c.get("href", "").strip():
                    href = _c["href"].strip()
                    break

            key = row[1]["text"]
            key = row[0]["text"].strip() + "|" + key
            glossary[key] = {"page": row[2]["text"], "href": href}

    return glossary


def get_section_boundary(glossary, section_name):
    section_boundary = [None, None]
    keys = list(glossary.keys())
    section = None
    for i, key in enumerate(keys):
        if section_name.lower() in key.lower() and not section_boundary[0]:
            section_boundary[0] = glossary[key]
            section = key.split("|")[0].strip(".").strip(",")
            continue
        if section_boundary[0]:
            _s = keys[i].split("|")[0].strip(".").strip(",").replace(section, "")
            if _s.strip() != "" and not re.search("^[a-zA-Z]{1}$", _s.strip()):
                section_boundary[1] = glossary[keys[i]]
        if section_boundary[0] and section_boundary[1]:
            break

    return section_boundary


def get_section_text(html_path, section_name):
    glossary = get_glossary(html_path)
    section_boundary = get_section_boundary(glossary, section_name)
    with open(html_path, "r") as f:
        soup = BeautifulSoup(f, "html.parser")
    section_text = ""
    if section_boundary[0] and section_boundary[1]:
        element = soup.find("a", {"name": section_boundary[0]["href"].lstrip("#")})
        while True:
            if element.next_sibling is None:
                element = element.parent
            else:
                element = element.next_sibling
            boundary_end = element.find("a", {"name": section_boundary[1]["href"].lstrip("#")})
            if boundary_end:
                sec_text = "\n".join(
                    [_e.get_text() for _e in boundary_end.previous_siblings if _e.name != "table"]
                )
                section_text = section_text + "\n" + sec_text
                break
            if element.name == "table":
                continue
            section_text = section_text + "\n" + element.get_text()

    return section_text


def get_table_paragraphs(node):
    # function to fetch paragraph which are organized inside table tag
    paragraphs = []
    table = extract_html_table(node)
    if not table['body']:
        return paragraphs
    for j in range(len(table['body'][0])):
        for i in range(len(table['body'])):
            if not table['body'][i][j]['text']:
                continue
            paragraphs.append(__get_node_obj(table['body'][i][j]['text'],
                                             table['body'][i][j]['cells']))
    return paragraphs


def __get_node_obj(text, span_nodes=[]):
    return {
        "text": text.strip(),
        "features": {
            "font-size": get_font_style(span_nodes, "font-size"),
            "font-weight": get_font_style(span_nodes, "font-weight"),
            "font-style": get_font_style(span_nodes, "font-style"),
        },
    }


def __iterate(parent):
    texts = []
    if parent.name == "table":
        return get_table_paragraphs(parent)
    
    if type(parent) != bs4.element.Tag:
        if str(parent).replace("\n", "").strip():
            texts.append(__get_node_obj(str(parent).strip()))
        return texts

    if not parent.children and parent.get_text().strip():
        texts.append(__get_node_obj(parent.get_text().strip()))
    else:
        text = ""
        span_nodes = []
        for node in parent.children:
            if node.name == "span" or node.name == "sup" or node.name == "font":
                text += node.get_text()
                span_nodes.append(node)
            else:
                if text.strip():
                    texts.append(__get_node_obj(text, span_nodes))
                texts += __iterate(node)
                text = ""
                span_nodes = []
        if text.strip():
            texts.append(__get_node_obj(text, span_nodes))
    return texts


def get_section_paragraphs(html_path, section_name):
    glossary = get_glossary(html_path)
    section_boundary = get_section_boundary(glossary, section_name)
    with open(html_path, "r") as f:
        soup = BeautifulSoup(f, "html.parser")
    section_paragraphs = []
    if section_boundary[0] and section_boundary[1]:
        element = soup.find(attrs={"name": section_boundary[0]["href"].lstrip("#")})
        if element is None:
            element = soup.find(attrs={"id": section_boundary[0]["href"].lstrip("#")})
        while element is not None:
            if element.next_sibling is None:
                element = element.parent
                continue
            else:
                element = element.next_sibling
            if type(element) != bs4.element.Tag:
                continue
            boundary_end = element.find(attrs={"name": section_boundary[1]["href"].lstrip("#")})
            if boundary_end is None:
                element.find(attrs={"name": section_boundary[1]["href"].lstrip("#")})
            if boundary_end:
                for _e in boundary_end.previous_siblings:
                    section_paragraphs += __iterate(_e)
                break
            section_paragraphs += __iterate(element)

    rank_nodes(section_paragraphs)
    return section_paragraphs


def generate_paragraphs(nodes):
    paragraphs = []
    for i, node in enumerate(nodes):
        rank = node["rank"]
        headings = []
        for upper_node in reversed(nodes[max(0, i - 50) : i]):
            if upper_node["rank"] > rank:
                rank = upper_node["rank"]
                headings.append(upper_node["text"])
            if len(headings) >= 3:
                break
        para = "\n\n".join(reversed(headings))
        para = para + "\n\n" + node["text"]
        paragraphs.append(para)
    return paragraphs


# function to extract html text, without losing heading hierarchy
def get_document_structure(html_path):
    def __get_node_obj(text, span_nodes=[]):
        return {
            "text": text.strip(),
            "features": {
                "font-size": get_font_style(span_nodes, "font-size"),
                "font-weight": get_font_style(span_nodes, "font-weight"),
                "font-style": get_font_style(span_nodes, "font-style"),
            },
        }

    def __iterate(parent, texts=[]):
        if type(parent) != bs4.element.Tag:
            if str(parent).replace("\n", "").strip():
                texts.append(__get_node_obj(str(parent).strip()))
            return

        if not parent.children and parent.get_text().strip():
            texts.append(__get_node_obj(parent.get_text().strip()))
        else:
            text = ""
            span_nodes = []
            for node in parent.children:
                if node.name == "span" or node.name == "sup" or node.name == "font":
                    text += node.get_text()
                    span_nodes.append(node)
                else:
                    if text.strip():
                        texts.append(__get_node_obj(text, span_nodes))
                    __iterate(node, texts)
                    text = ""
                    span_nodes = []
            if text.strip():
                texts.append(__get_node_obj(text, span_nodes))

    with open(html_path, "r") as f:
        soup = BeautifulSoup(f, "html.parser")

    data = []
    __iterate(soup.html, data)
    rank_nodes(data)
    # generating paragraphs with heading information
    paragraphs = generate_paragraphs(data)
    with open("debug.json", "w", encoding="utf-8") as f:
        json.dump(data, f, indent=4)

    with open("debug.txt", "w", encoding="utf-8") as f:
        sep = "\n" + "-" * 50 + "\n"
        f.write(sep.join(paragraphs))


def get_font_style(nodes, style_name):
    if len(nodes) == 0:
        return ""
    node = nodes[0]
    style = node.get("style")
    styles = style.split(";")
    for style in styles:
        if style_name in style:
            return style.replace(style_name + ":", "")
    return ""


def rank_nodes(nodes):
    for node in nodes:
        try:
            rank = float(re.search("\d{1,}\.?\d{0,}", node["features"]["font-size"]).group())
        except:
            rank = 0
        if node["features"]["font-weight"] == "bold":
            rank += 2
        if node["features"]["font-style"] == "italic":
            rank += 1
        node["rank"] = rank


if __name__ == "__main__":
    html_path = r"data\Current\AIG/000000527223000007/aig-20221231.htm"
    # get_document_structure(html_path)
    section_paragraphs = get_section_paragraphs(html_path, "Business")
    # with open('test.txt', 'w', encoding='utf-8') as f:
    #     f.write(section_text)
    # data = extract_html_tables(html_path)
    with open("debug.json", "w") as f:
        json.dump(section_paragraphs, f, indent=4)
