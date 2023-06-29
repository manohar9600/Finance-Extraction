import fitz
import openai
import json
import os
from loguru import logger


openai.api_key = "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"


def get_finance_pages(pdf_path):
    MODEL = "gpt-3.5-turbo"
    prompt = "Classify given text as BalanceSheet or IncomeStatement or Cashflow or None. don't explain."
    system_msg = f"You are a helpful assistant. Help me find right type of content in this pdf page. {prompt}"

    file_name = os.path.splitext(os.path.basename(pdf_path))[0]
    final_output = {
        'file': file_name,
        'textblocks': []
    }
    doc = fitz.open(pdf_path)
    for page in doc:
        logger.info(f'processing page:{page.number+1}')
        text = page.get_text()
        
        response = openai.ChatCompletion.create(
            model=MODEL,
            messages=[
                {"role": "system", "content": system_msg},
                {"role": "user", "content": text},
            ],
            temperature=0,
        )
        final_output['textblocks'].append({
            'page': page.number+1,
            'text': text,
            'gptoutput': response['choices'][0]['message']['content']
        })

    doc.close()
    return final_output


if __name__ == "__main__":
    output = get_finance_pages("PDFs/Amazon-2022-Annual-Report.pdf")
    os.makedirs('data', exist_ok=True)
    with open(f'data/{output["file"]}.json', 'w') as f:
        json.dump(output, f, indent=4)