from extraction.html_functions import get_section_html
from extraction.gpt_functions import get_gpt_answer
from extraction.db_functions import VectorDBFunctions


def get_company_segments(html_path):
    # code to get business section html
    section_html = get_section_html(html_path, "business") 
    relevant_docs = VectorDBFunctions().get_relevant_documents_html(
        section_html, "what are the company's products and services and segments")
    context = "\n".join(relevant_docs)
    question = "Find and extract company's products, services, segments and don't include other types."
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
