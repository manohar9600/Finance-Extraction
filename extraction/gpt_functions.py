import openai
import re
import time
from loguru import logger


openai.api_key = "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"


def generate_markdown_table(columns, table_body):
    header_row = "| " + " | ".join(columns) + " |"
    separator_row = "| " + " | ".join(['---'] * len(columns)) + " |"
    body_rows = ["| " + " | ".join(str(item) for item in row) + " |" for row in table_body]
    return "\n".join([header_row, separator_row] + body_rows)


def check_gpt_match(text1, text2):
    prompt = f'is "{text1}" and "{text2}" are same ? say yes or no'
    try:
        response = openai.ChatCompletion.create(
            model="gpt-3.5-turbo",
            messages=[
                {"role": "user", "content": prompt},
            ],
            temperature=1,
        )
        if 'yes' in response['choices'][0]['message']['content'].lower()[:5]:
            return True
    except:
        logger.info('failed to get response from openai api. sleeping for 5 secs')
        time.sleep(5)
        return check_gpt_match(text1, text2)
    return False


def get_value_gpt(variable, document_period, excel_tables, retry_count=0):
    for table in excel_tables:
        markdown_table = generate_markdown_table(table['header'], table['body'])
        prompt = f'{markdown_table}\nwhat is the value of {variable["variable"]} of period {document_period}. output should contain only value along with quantum and if it is debit value change to absolute value. if there is no value just return "no".'
        try:
            response = openai.chat.completions.create(
                model="gpt-4-1106-preview",
                messages=[
                    {"role": "user", "content": prompt},
                ]
            )
            result = response.choices[0].message.content
            if 'no' in result:
                continue
            try:
                result = word_to_num(result)
                return result
            except:
                continue
        except Exception as e:
            logger.error(f'error from openai function: {e}')
            if retry_count < 2:
                logger.warning('failed to get response from openai api. sleeping for 5 secs')
                time.sleep(5)
                return get_value_gpt(variable, document_period, excel_tables, retry_count+1)
            else:
                logger.error('failed to get response from openai api. sending out empty value')
    return None


def word_to_num(s):
    multipliers = {
        'thousand': 1000,
        'million': 1000000,
        'billion': 1000000000,
        'trillion': 1000000000000
    }
    words = s.replace(",", "").lower().split()
    if len(words) > 1:
        total = float(words[0]) * multipliers[words[1]]
    else:
        total = float(words[0])
    return "{:,}".format(total)