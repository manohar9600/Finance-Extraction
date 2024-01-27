from extraction.html_functions import get_section_html, get_pages_text
from tqdm import tqdm
import pandas as pd
from io import StringIO
from extraction.gpt_functions import summarize, get_multivector_retriver, get_gpt_answer


def get_relevant_documents(section_html):
    # todo remove dependency on file save
    with open("debug.html", "w", encoding="utf-8") as file:
        file.write(str(section_html))

    data_texts = get_pages_text("debug.html")
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
    return relevant_docs


def get_company_segments(html_path):
    # code to get business section html
    section_html = get_section_html(html_path, "business")
    relevant_docs = get_relevant_documents(section_html)
    context = "\n".join(relevant_docs)
    question = "what are the company's products and services and segments."
    prompt = f"""context: {context}
                    prompt: {question}
                    ouput format: tabluar format, columns are item, type, description"""
    result = get_gpt_answer(prompt)
    df = pd.read_csv(StringIO(result),  sep="|", index_col=1).dropna(axis=1, how="all").iloc[1:]
    return df


if __name__ == "__main__":
    html_path = "data/Current/AAPL/000032019320000096/aapl-20200926.htm"
    products_df = get_company_segments(html_path)
    print(products_df)
