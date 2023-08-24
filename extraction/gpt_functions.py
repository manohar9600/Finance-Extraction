import openai
import time
from loguru import logger


openai.api_key = "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"


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