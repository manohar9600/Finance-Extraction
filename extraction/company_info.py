from extraction.html_functions import get_section_html, get_pages_text
from tqdm import tqdm
from extraction.gpt_functions import summarize, get_multivector_retriver, get_gpt_answer
from loguru import logger


def get_relevant_documents(section_html):
    data_texts = get_pages_text(str(section_html))
    descriptions = []
    for text in tqdm(data_texts):
        descriptions.append(summarize(text))

    # adding page number to sort them in context
    data_texts = [a + "||" + str(i) for i, a in enumerate(data_texts)]

    retriever = get_multivector_retriver(data_texts, descriptions)
    relevant_docs = retriever.get_relevant_documents(
        "what are the company's products and services and segments"
    )
    relevant_docs = sorted(relevant_docs, key=lambda x: int(x.split("||")[-1]))
    logger.debug(f"RAG selected pages: {','.join([x.split('||')[-1] for x in relevant_docs])}")
    return relevant_docs


def get_company_segments(html_path):
    # code to get business section html
    section_html = get_section_html(html_path, "business")
    relevant_docs = get_relevant_documents(section_html)
    context = "\n".join(relevant_docs)
    question = "what are the company's products and services and segments."
    prompt = f"""context: {context}
                    prompt: {question}
                    output format: Markdown table. columns should be Item(product or service or segment name), Category, Text related to it."""
    result = get_gpt_answer(prompt)
    segments_data = {}
    for l in result.split('\n')[2:]:
        item, item_type, description = [s.strip() for s in l.strip('|').split('|')]
        if item_type not in segments_data:
            segments_data[item_type] = []
        segments_data[item_type].append({'name': item, 'description': description})
    return segments_data


if __name__ == "__main__":
    html_path = "data/Current/AAPL/000032019320000096/aapl-20200926.htm"
    products_df = get_company_segments(html_path)
    print(products_df)
