import glob
import json
import datetime
from typing import List, Any

from pyscgen.json.analyze.analyze_documents import JSONAnalyzer


class JSONEncoder(json.JSONEncoder):
    """
    JSONEncoder to output various types.
    """
    def default(self, o):
        if isinstance(o, datetime.datetime):
            return o.isoformat()
        if isinstance(o, type):
            return str(o)
        if o == Any:
            return o.__dict__
        return json.JSONEncoder.default(self, o)


# get at least one json documente, load it and put it in a list
json_documents: List[dict] = []
files: list = glob.glob(pathname="./data/*.json")
i = 0
for file_path in files:
    with open(file_path, "r") as file:
        json_ = json.load(file)
        i += 1
        print("working on row " + str(i))
        json_documents.append(json_)

# initialize the JSONAnalyzer
json_analyzer: JSONAnalyzer = JSONAnalyzer()
# let it run
collection_data, column_infos, df_flattened, df_dtypes, df_unique = json_analyzer.analyze(json_documents)

# writing the output so you can see the results or do whatever you like
with open("./out/column_infos.json", "w+") as file:
    json.dump(json.loads(JSONEncoder().encode(column_infos.as_dict())), file, indent=4)
with open("./out/collection_data.json", "w+") as file:
    json.dump(json.loads(JSONEncoder().encode(collection_data.as_dict())), file, indent=4)
df_flattened.to_csv("./out/df_flattened.csv")
df_dtypes.to_csv("./out/df_dtypes.csv")
df_unique.to_csv("./out/df_unique.csv")