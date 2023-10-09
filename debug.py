import os
import json
from datetime import datetime
from glob import glob


for comp_path in glob(r"data\current\*"):
    result = ''
    for xbrl_path in glob(os.path.join(comp_path, "*/xbrl_data.json")):
        with open(xbrl_path, 'r') as f:
            xbrl_data = json.load(f)

        for dp in xbrl_data.get('factList', []):
            if dp[2]["label"] == "dei:DocumentPeriodEndDate":
                result = datetime.strptime(dp[2]["value"], "%Y-%m-%d").strftime("%m-%d")
                break
        if result:
            break
    print(f"sym: {comp_path}, res: {result}")

