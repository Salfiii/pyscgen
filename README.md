# (pyscgen) python schema generator 
A Python Package to analyze JSON Documents, build a merged JSON-Document out of multiple provided JSON Documents and create an 
AVRO-Schemas based on multiple given JSON Documents.

## Installation

### pip
``pip install pyscgen``

### poetry
``poetry add pyscgen``

## JSON Analyzer
receives a list of json documents, analyzes the structure and outputs the following infos:

-**Output**:
- collection_data: 
  - dict which stores infos about the columns of each document with attributes like name, path, data type and if its null
- column_infos: 
  - condensed/merged column infos of all given documents with attributes like name, path, nullability, density, unique values, data types found, parent column config etc.
  - This is the "real" result of the analyzer and the building plan for the JSON Merger and AVRO Schema generator.
- df_flattened
  - A pandas DataFrame which stores the json documents flattened and contains every found column with data.
  - One document is represented by one row, index starts at 0 and matches to the order in the given list of documents.
- df_dtypes
  - A pandas DataFrame which stores the python data type for each column of all given documents.
  - One document is represented by one row, index starts at 0 and matches to the order in the given list of documents.
- df_unique
  - A pandas DataFrame which stores unique values found for each column of all given documents.
  - This data frame is pivoted
    - The column "0" stores all found column. One column is represented by one row.
    - The columns 1 - n contain one distinct value each. If you analyze 10 documents and one field has a distinct value in each document, you´ll produce 10 value columns.

## JSON Merger
receives a list of json documents, analyzes the structure with the JSON Analyzer and outputs one merged dictionary/json document with all found columns and dummy values according to the found data types:
 
- **Output**: merged_doc: 
  - dictionary merged together from the all given JSON documents.

## AVRO Schema generator
receives a list of json documents, analyzes the structure with the JSON Analyzer and outputs a "Schema"-object which can be converted to a dict and stored in an avsc-file
 
- **Flow:**
  - JSON Analyzer -> JSON Merger -> AVRO Schema generator
- **Output**: avro_schema:
  - AVRO Schema object can be converted to a dict and stored to an avsc file, see examples.
- **Limitations/Currently not supported**:
  - Resolution of duplicated names in the AVRO-Output
    - You can resolve this manually by renaming duplicated elements by hand afterwards and use [aliases](https://avro.apache.org/docs/current/spec.html#Aliases) to still match with the input data.
    - You can also use inheritance in AVRO-Schemas, here´s an example: https://www.nerd.vision/post/reusing-schema-definitions-in-avro, but that´s definitely more complicated
  - empty dicts/maps:
    - If one of your JSON input documents has an empty dictionary, e. g. ```{"field1": "value1", "field2": {}}``` an empty AVRO record will be generated, which is in valid in AVRO.
    - You can resolve this by deleting the empty record or filling it with live if you know that it´s still needed in the future.
  - Mixed types for one field in the the JSON Input documents currently results in the most generic AVRO type, which ist a String (Union types, as an option, is planned)
    - This leads to the case that not all input documents automaticall validate positively against the AVRO schema, because you need to convert types beforehand. 
    - You can use the pydantic model generator to create a model, which automatically converts other data types to string, to solve this issue. Just let all your JSON flow through the model bevore validation.

## Pydantic Model generator
receives a list of json documents analyzes the structure with the JSON Analyzer, creates an AVRO Schema which is then used to create a pydantic Model with [pydantic-avro](https://github.com/godatadriven/pydantic-avro)
 
- **Flow**
  - JSON Analyzer -> JSON Merger -> AVRO Schema generator -> Pydantic Model generator 
- **Output:** pydantic model:
  - Pydantic Model as a string which can be written to a .py-File.
- **Limitations/Currently not supported**:
  - Python/AVRO bytes is currently not supported in pydantic model generation because there is no support in pydantic for this datatype right now.

# Why should you use it?
Considering the fact that there are already other AVRO Schema generators, why should you use this one?

The simple answer is "null" or "nullability".

All other solutions I tried simply let you pass one JSON Document as a base for the AVRO-Schema generation.
This does obviously not allow to infer nullabilty, because only what's present in this one message can be observed 
and therefore used for schema generation.

This library lets you pass a list of JSON Documents and therefore can gather the infos which fields are always 
present - aka. mandatory - and which fields are only found in a couple documents - and therefore nullable.

Handling nullability in AVRO-Schemas for records and arrays is quite painful in my opinion, 
this library gets rid of this chore for you.

Of course, it works just fine with only one JSON document like any other AVRO generator, just without the nullability part then.

# Principles
- CI-CO (Crap in - Crap out): 
  - The JSON Documents are not validated, therefore, if you pass in garbage, crap in - crap out.
  - The goal is to creata an AVRO-Schema no matter what. In my opinion, an half ready AVRO-Schema which needs to be edited by hand is better than no AVRO-Schema at all!

# Examples:
- [JSON Analyzer](./example/pyscgen_json_analyze.py)
- [JSON Merger](./example/pyscgen_json_merge.py)
- [AVRO Schema creator](./example/pyscgen_avro_create_schema.py)
- [Pydantic model creator](./example/pyscgen_pydantic_create_schema.py)

# Other useful resources regarding AVRO:
- [Official Apache AVRO docs](https://avro.apache.org/docs/current/spec.html)
- [Python AVRO Schema valdiator "fastavro"](https://github.com/fastavro/fastavro)

# License:
[BSD-3-Clause](LICENSE)