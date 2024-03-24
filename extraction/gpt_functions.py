import openai
import time
from loguru import logger
from langchain_openai import OpenAIEmbeddings
from mistralai.client import MistralClient
from mistralai.models.chat_completion import ChatMessage
import google.generativeai as genai
from langchain_mistralai.chat_models import ChatMistralAI
from langchain_core.messages import HumanMessage, SystemMessage
from langchain_anthropic.experimental import ChatAnthropicTools
from langchain_openai import ChatOpenAI


# todo change to system variable and remove open_ai_key
open_ai_key = "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"
openai.api_key = open_ai_key
mistral_api_key = "XjDKSArDspNO81zsPXIFIQd06ib3x7nJ"


haiku_llm = ChatAnthropicTools(
    anthropic_api_key="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",
    model="claude-3-haiku-20240307")


gpt4_llm = ChatOpenAI(
    openai_api_key="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",
    model="gpt-4-0125-preview" # gpt-3.5-turbo-0125
)


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
        prompt = f'{markdown_table}\nwhat is the value of {variable["variable"]} of period {document_period}. output should contain only value along with quantum. if there is no value just return "no".'
        try:
            response = openai.chat.completions.create(
                model="gpt-3.5-turbo",
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


def get_gpt_answer(prompt, model="gpt-3.5-turbo-0125"):
    response = openai.chat.completions.create(
        model=model,
        messages=[
            {"role": "user", "content": prompt},
        ],
    temperature=1
    )
    result = response.choices[0].message.content
    return result


def get_gpt_answer_json(prompt, model="gpt-3.5-turbo-0125"):
    response = openai.chat.completions.create(
        model=model,
        response_format={ "type": "json_object" },
        messages=[
            {"role": "system", "content": "You are a helpful assistant designed to output JSON."},
            {"role": "user", "content": prompt},
        ],
    temperature=1
    )
    result = response.choices[0].message.content
    return result


# def get_gpt_answer(prompt):
#     response = openai.chat.completions.create(
#         model="gpt-4-1106-preview",
#         messages=[
#             {"role": "user", "content": prompt},
#         ],
#     temperature=1
#     )
#     result = response.choices[0].message.content
#     return result


def summarize(text):
    messages = [
       SystemMessage(content="Your job is to describe what given text tells about the company. With proper document structure with headings and subheadings. Output should not exceed 150 words. Also write questions that can be asked to fetch results from given text."),
       HumanMessage(content=text)
    ]
    chat = ChatMistralAI(
        mistral_api_key=mistral_api_key, model="mistral-small-latest"
    )
    return chat.invoke(messages).content


def get_openai_embeddingfn():
    return OpenAIEmbeddings(
                model="text-embedding-ada-002", openai_api_key=open_ai_key
            )


def get_mistral_answer(prompt, model, system_prompt=""):
    client = MistralClient(api_key=mistral_api_key)
    messages = []
    if system_prompt:
        messages.append(ChatMessage(role="user", content=system_prompt))
    messages.append(ChatMessage(role="user", content=prompt))
    # No streaming
    chat_response = client.chat(
        model=model,
        messages=messages,
    )
    return chat_response.choices[0].message.content


def get_gemini_answer(prompt):
    google_ai_api_key = "AIzaSyCDEkFzRpctmGFXY1OfOGqC96MnZKfnTm4"
    genai.configure(api_key=google_ai_api_key)
    model = genai.GenerativeModel('gemini-pro')
    return model.generate_content(prompt).text