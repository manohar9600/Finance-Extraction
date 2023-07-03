from tabulate import tabulate

# Markdown table string
markdown_table = '''
|                           | September 29, 2018 | September 29, 2017 |
|---------------------------|-------------------:|-------------------:|
| Current assets            |                    |                    |
| Cash and cash equivalents |            $25,913 |            $20,289 |
| Marketable securities     |            $40,388 |            $53,892 |
| Accounts receivable, net  |            $23,186 |            $17,874 |
| Inventories               |             $3,956 |             $4,855 |
| Vendor non-trade receivables |          $25,809 |            $17,799 |
| Other current assets      |            $12,087 |            $13,936 |
| Total current assets      |           $131,339 |           $128,645 |
| Non-current assets        |                    |                    |
| Marketable securities     |           $170,799 |           $194,714 |
| Property, plant and equipment, net |   $41,304 |            $33,783 |
| Other non-current assets  |            $22,283 |            $18,177 |
| Total non-current assets  |           $234,386 |           $246,674 |
| Total assets              |           $365,725 |           $375,319 |
| Liabilities and Shareholders' Equity |         |                    |                    |
| Current liabilities       |                    |                    |
| Accounts payable          |            $55,888 |            $44,242 |
| Other current liabilities |            $32,687 |            $30,551 |
| Deferred revenue          |             $7,543 |             $7,548 |
| Commercial paper          |            $11,964 |            $11,977 |
| Term debt                 |             $8,784 |             $6,496 |
| Total current liabilities |           $116,866 |           $100,814 |
| Non-current liabilities   |                    |                    |
| Deferred revenue          |             $2,797 |             $2,836 |
| Term debt                 |            $93,735 |            $97,207 |
| Other non-current liabilities |          $45,180 |            $40,415 |
| Total non-current liabilities |         $141,712 |           $140,458 |
| Total liabilities         |           $258,578 |           $241,272 |
| Shareholders' equity      |                    |                    |
| Common stock and additional paid-in capital | $40,201 |   $35,867 |
| Retained earnings         |            $70,400 |            $98,330 |
| Accumulated other comprehensive income/(loss) | ($3,454) |   ($150) |
| Total shareholders' equity |           $107,147 |           $134,047 |
| Total liabilities and shareholders' equity |   $365,725 |   $375,319 |
'''

# Splitting markdown table into rows
rows = markdown_table.strip().split('\n')

# Splitting header and data rows
header = [cell.strip() for cell in rows[0].split('|')[1:-1]]
data = [list(map(str.strip, row.split('|')[1:-1])) for row in rows[2:]]

print('done')
