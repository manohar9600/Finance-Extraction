from glob import glob
import json
import openai


openai.api_key = "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"


data = []

def get_index(tables, cls):
    text = "\n---\n".join([t['textAbove'].replace("\n", " ") for t in tables])
    system_msg = f"in given text which is final statement heading. just output the index of above list."
    response = openai.ChatCompletion.create(
        model="gpt-3.5-turbo",
        messages=[
            {"role": "system", "content": system_msg},
            {"role": "user", "content": text},
        ],
        temperature=0,
    )
    cls = response['choices'][0]['message']['content']
    return cls



for file in glob(r"data\aapl\*\tables.json"):
    with open(file, "r") as f:
        tables = json.load(f)['tables']
    dd = {}
    for table in tables:
        if table['class']:
            if table['class'] not in dd:
                dd[table['class']] = []
            dd[table['class']].append(table)
    
    for cls in dd:
        if len(dd[cls]) < 1:
            continue

        index = get_index(dd[cls], cls)
        for i, table in enumerate(dd[cls]):
            if i == index - 1:
                continue
            dd[cls][i] = ""
    
    print("done")