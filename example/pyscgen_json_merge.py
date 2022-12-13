import glob
import itertools
import json
import datetime
from typing import List, Any

from pyscgen.json.merge.merge_documents import DocumentMerger

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
document_merger: DocumentMerger = DocumentMerger()
# let it run
merged_doc: dict = document_merger.get_merged_document(json_documents)
# writing the output so you can see the results or do whatever you like
with open("./out/merged_doc.json", "w+") as file:
    json.dump(merged_doc, file, indent=4)