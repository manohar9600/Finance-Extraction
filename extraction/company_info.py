import json
from extraction.html_functions import get_section_html
from extraction.gpt_functions import get_mistral_answer, get_gpt_answer_json
from extraction.db_functions import VectorDBFunctions
from tqdm import tqdm
from loguru import logger
from langchain.prompts import PromptTemplate
from langchain_core.output_parsers import JsonOutputParser
from langchain_core.pydantic_v1 import BaseModel, Field
from langchain_openai import ChatOpenAI
from langchain_mistralai.chat_models import ChatMistralAI


def get_company_segments_v2(html_path):
    # model = ChatOpenAI(
    #     openai_api_key="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",
    #     model="gpt-3.5-turbo-0125"
    # )
    model = ChatMistralAI(
        mistral_api_key="XjDKSArDspNO81zsPXIFIQd06ib3x7nJ", model="open-mixtral-8x7b"
    )

    # Define your desired data structure.
    class ProductsInfo(BaseModel):
        # name: list = Field(description="company name")
        Products: list = Field(
            description="products produced by the company. just product or service or solution or operation name"
        )
        # Services: list = Field(description="services offered by the company. just service name")

    parser = JsonOutputParser(pydantic_object=ProductsInfo)

    prompt = PromptTemplate(
        template="Answer the user query.\n{format_instructions}\n{query}\n",
        input_variables=["query"],
        partial_variables={"format_instructions": parser.get_format_instructions()},
    )

    chain = prompt | model | parser

    # code to get business section html and RAG
    section_html = get_section_html(html_path, "business") 
    relevant_docs = VectorDBFunctions().get_relevant_documents_html(
        section_html, f"what are the company's products and services and operations and solutions?")

    # ---
    reduced_content = []
    for doc in tqdm(relevant_docs, desc="reducing number of tokens"):
        prompt = f"Document: \n {doc} \n---\n Given a document with distinct headings and a structured format, please compress the text under each heading. The compression should aim to reduce the overall length of the text by eliminating redundant information and simplifying sentences, while focusing on key points. It's crucial to maintain the integrity of the document's structure, including all headings, subheadings, and any bullet points or numbered lists. Ensure to explicitly retain all product and service and operations and solution names mentioned in the text. The goal is to create a concise version of the document that retains all critical information, including product and service andand operations  solution names, and remains easy to navigate. Please ensure the compressed text under each heading is coherent, directly related to the heading, and that the transition between sections is smooth, with a particular emphasis on preserving the mention and context of all product names."
        reduced_content.append(get_mistral_answer(prompt, "open-mixtral-8x7b"))

    products_context = "\n---\n".join(reduced_content)
    ques = "what are products and services and solutions and operations names offered or produced by the company. don't include description."
    prompt = f"{products_context} \n --- \n {ques}"
    with open('prompt.txt', 'w') as f:
        f.write(prompt) 

    logger.info('generating final products info...')
    output = chain.invoke({"query": prompt})

    # normalising ouput to single structure
    final_output = {}
    for key in output:
        final_output[key] = []
        for p in output[key]:
            final_output[key].append({
                "name": p,
                "description": "",
            })

    return final_output


def get_company_segments(html_path):
    # code to get business section html
    section_html = get_section_html(html_path, "business") 
    relevant_docs = VectorDBFunctions().get_relevant_documents_html(
        section_html, "what are the company's products")

    page_products = []
    for page in tqdm(relevant_docs, desc="fetching products"):
        ques = "you are smart document reader. You're job is to extract company's products that are mentioned in above context."
        prompt = f"context:{page}\n---\n{ques}"
        page_products.append(get_mistral_answer(prompt, "mistral-tiny"))
    
    products_context = "\n---\n".join(page_products)
    ques = "you're job is combine products from each section. Product name should be brief. output format -> {\"products\": [{\"name\": product name} for each product]}"
    prompt = f"{products_context}\n---\n{ques}"
    with open('prompt.txt', 'w') as f:
        f.write(prompt)
    logger.info('generating final products info...')
    output_data = json.loads(get_gpt_answer_json(prompt))
    return output_data

# def get_company_segments(html_path):
#     # code to get business section html
#     section_html = get_section_html(html_path, "business")
#     relevant_docs = VectorDBFunctions().get_relevant_documents_html(
#         section_html, "what are the company's products and services and segments")

#     reduced_content = []
#     for doc in tqdm(relevant_docs, desc="reducing number of tokens"):
#         prompt = f"{doc} \n---\n Reduce number of tokens without losing key information. retain products and services and segments information."
#         reduced_content.append(get_mistal_answer(prompt, "mistral-tiny"))

#     context = "\n".join(reduced_content)
#     question = "Find and extract company's products, services, segments and don't include other types."
#     prompt = f"""context: {context}
#         prompt: {question}
#         output format: Markdown table. columns should be Item(product or service or segment name), Category(product or service or segment), Text related to it."""

#     # result = get_gpt_answer(prompt, "gpt-4-1106-preview")
#     with open('prompt.txt', 'w') as f:
#         f.write(prompt)
#     logger.info('generating products info...')
#     result = get_gemini_answer(prompt)

#     segments_data = {}
#     for l in result.split('\n')[2:]:
#         item, item_type, description = [s.strip() for s in l.strip('|').split('|')]
#         if item_type not in segments_data:
#             segments_data[item_type] = []
#         segments_data[item_type].append({'name': item, 'description': description})
#     return segments_data


if __name__ == "__main__":
    html_path = "data/Current/AAPL/000032019320000096/aapl-20200926.htm"
    products_df = get_company_segments(html_path)
    print(products_df)
