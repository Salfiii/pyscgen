import glob
import json
import fastavro
from typing import List

from pyscgen.avro._model.record_model import Schema
from pyscgen.avro.schema.create_schema import AvroSchemaGenerator


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
avro_generator: AvroSchemaGenerator = AvroSchemaGenerator()
# let it run and create a schema object
avro_schema: Schema = avro_generator.create_schema(json_documents)

# NOTE: use the internal function and not anything else because it will remove empty attributes which might
# otherwise lead to an invalid AVRO Schema!
avro_schema_dict: dict = avro_schema.as_dict()

# writing the output so you can see the results or do whatever you like
schema_path: str = "./out/avro_schema.avsc"
with open(schema_path, "w+") as file:
    json.dump(avro_schema_dict, file, indent=2)

# if needed, validate the avro schema based on some data
with open(schema_path, "r") as value_schema_file:
    value_schema_str = value_schema_file.read()

# if you want to, try to validate the data
for i in range(0, len(json_documents)):
    fastavro.validation.validate(json_documents[i], fastavro.parse_schema(json.loads(value_schema_str)))
