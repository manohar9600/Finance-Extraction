import os
import json
from intervaltree import IntervalTree, Interval
from bs4 import BeautifulSoup


def convert_intervaltree(table_tree):
    # function to convert interval tree data to list data
    num_rows = max([len(interval.data) for interval in sorted(table_tree)], default=0)
    table_body = [[] for _ in range(num_rows)]
    for interval in sorted(table_tree):
        for i, cell in enumerate(interval.data):
            if cell is not None:
                # table_body[i].append(cell.get_text())
                table_body[i].append(" ".join([_c.get_text().strip() for _c in cell]))
            else:
                table_body[i].append("")
    return table_body


def divide_column_rows(table_rows):
    column_index = 0
    multi_column_text = ""
    for row in table_rows:
        non_variable_text = ""
        for _c in row.find_all('td')[1:]:
            if _c.find_all('ix:nonfraction'):
                break
            non_variable_text += _c.get_text()
        else:
            if multi_column_text and not non_variable_text.strip() and row.get_text().strip():
                break
            col_texts = [t.get_text().strip() for t in row.find_all('td') if t.get_text().strip()]
            if len(col_texts) > 1:
                multi_column_text = multi_column_text + " ".join(col_texts)
            column_index += 1
            continue
        break
    
    return table_rows[:column_index], table_rows[column_index:]


def extract_html_table(table_tag):
    table_tree = IntervalTree()
    column_rows, table_rows = divide_column_rows(table_tag.find_all('tr'))
    num_rows = 0
    for row in column_rows[-1:] + table_rows: # adding last row of column header to get proper column coordinates
        prev_coord = 0
        table_cells = row.find_all('td')
        for cell in table_cells:
            # len(table_cells) == 1 condition for centered variables.
            if not cell.get('colspan') or len(table_cells) == 1:
                end_coord = prev_coord + 1
            else:
                end_coord = prev_coord + int(cell.get('colspan'))
            
            if len(cell.get_text().strip()) == 0:
                prev_coord = end_coord
                continue

            overlap_intervals = table_tree.overlap(prev_coord, end_coord)
            if not overlap_intervals:
                table_tree.add(Interval(prev_coord, end_coord, [[cell]]))
            else:
                interval = sorted(overlap_intervals)[0]
                data = interval.data
                table_tree.remove(interval)
                if len(data) > num_rows:
                    data[-1].append(cell)
                else:
                    data.append([cell])
                table_tree.add((Interval(
                    min(prev_coord, interval.begin), max(end_coord, interval.end), data)))
            prev_coord = end_coord
        
        # equating length of all columns
        num_rows = max([len(interval.data) for interval in sorted(table_tree)], default=0)
        for interval in table_tree:
            for _ in range(len(interval.data), num_rows):
                interval.data.append(None)

    table_dict = {
        'textAbove': "",
        'columns': [], #TODO
        'body': convert_intervaltree(table_tree)[1:] # 0th index is last row of column, so ignoring
    }
    return table_dict


def extract_html_tables(html_path):
    with open(html_path, 'r') as f:
        soup = BeautifulSoup(f, 'html.parser')

    tables = []
    table_tags = soup.find_all('table')
    for table_tag in table_tags:
        table = extract_html_table(table_tag)
        if table['body']:
            tables.append(table)
    
    return tables


if __name__ == "__main__":
    html_path = r"data\current\ACGL\000094748423000015\acgl-20221231.htm"
    data = extract_html_tables(html_path)
    with open("test.json", "w") as f:
        json.dump(data, f, indent=4)