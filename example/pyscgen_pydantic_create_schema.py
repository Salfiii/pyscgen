import glob
import json
import fastavro
from typing import List

from pyscgen.pydantic.schema.create_schema import PydanticSchemaGenerator


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


# initialize the Generator
pydantic_generator: PydanticSchemaGenerator = PydanticSchemaGenerator()
# let it run and create a schema object
pydantic_schema = pydantic_generator.create_schema(json_documents)

# writing the output so you can see the results or do whatever you like
schema_path: str = "./out/pyndantic_model.py"
with open(schema_path, "w+") as file:
    file.write(pydantic_schema)
