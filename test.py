# from secedgar import FilingType, CompanyFilings
# from datetime import date

# filing = CompanyFilings(cik_lookup="aapl",
#                         filing_type=FilingType.FILING_10K,
#                         start_date=date(2015, 1, 1),
#                         end_date=date(2019, 1, 1),
#                         user_agent="Name (email)")

# company_filings_urls = filing.get_urls()


items_list = [
            '1', '1A', '1B', '2', '3', '4', '5', '6', '7', '7A',
            '8', '9', '9A', '9B', '10', '11', '12', '13', '14', '15'
        ]


with open('0000320193-18-000145.html', 'r') as f:
    data = f.read()

# from extract_items import ExtractItems

# text = ExtractItems.strip_html(str(data))
# text = ExtractItems.clean_text(text)

# json_content = {}
# positions = []
# all_items_null = True
# for i, item_index in enumerate(items_list):
#     next_item_list = items_list[i+1:]
#     item_section, positions = ExtractItems.parse_item(text, item_index, next_item_list, positions)
#     item_section = ExtractItems.remove_multiple_lines(item_section)

#     if item_index in items_list:
#         if item_section != '':
#             all_items_null = False
#         json_content[f'item_{item_index}'] = item_section


# print('done')

import re

matches = re.findall("Item 8.+Item 9", data)

with open('match.txt', 'w') as f:
    f.write(matches[0])