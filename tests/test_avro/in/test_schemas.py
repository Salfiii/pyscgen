import json

import fastavro
import pytest
from fastavro._validation import ValidationError


class TestSchemas:


    def test_simple_schema(self):
        schema_path: str = "./simple_test_schema.avsc"
        # Einlesen des ersten gefundenen AVRO-Schemas
        with open(schema_path, "r") as value_schema_file:
            value_schema_str = value_schema_file.read()
        # Laden & Parsen der schemas
        value_schema = fastavro.schema.load_schema(schema_path)

        with open("./data/simple_merged.json", "r") as file:
            json_ = json.load(file)

        fastavro.validation.validate(json_, fastavro.parse_schema(json.loads(value_schema_str)))

    def test_complex_schema(self):
        schema_path: str = "./complex_test_schema.avsc"
        # Einlesen des ersten gefundenen AVRO-Schemas
        with open(schema_path, "r") as value_schema_file:
            value_schema_str = value_schema_file.read()
        # Laden & Parsen der schemas
        value_schema = fastavro.schema.load_schema(schema_path)

        with open("./data/complex_merged.json", "r") as file:
            json_ = json.load(file)

        fastavro.validation.validate(json_, fastavro.parse_schema(json.loads(value_schema_str)))

    def test_nullable_array_and_record_schema(self):
        schema_path: str = "./nullable_array_and_record_schema.avsc"
        # Einlesen des ersten gefundenen AVRO-Schemas
        with open(schema_path, "r") as value_schema_file:
            value_schema_str = value_schema_file.read()
        # Laden & Parsen der schemas
        value_schema = fastavro.schema.load_schema(schema_path)

        with open("./data/complex_merged.json", "r") as file:
            json_ = json.load(file)

        fastavro.validation.validate(json_, fastavro.parse_schema(json.loads(value_schema_str)))

    def test_error_schema_simple(self):
        schema_path: str = "./simple_test_schema.avsc"
        # Einlesen des ersten gefundenen AVRO-Schemas
        with open(schema_path, "r") as value_schema_file:
            value_schema_str = value_schema_file.read()
        # Laden & Parsen der schemas
        value_schema = fastavro.schema.load_schema(schema_path)

        with open("./data/error_data.json", "r") as file:
            json_ = json.load(file)
        with pytest.raises(ValidationError):
            fastavro.validation.validate(json_, fastavro.parse_schema(json.loads(value_schema_str)))

    def test_error_schema_complex(self):
        schema_path: str = "./complex_test_schema.avsc"
        # Einlesen des ersten gefundenen AVRO-Schemas
        with open(schema_path, "r") as value_schema_file:
            value_schema_str = value_schema_file.read()
        # Laden & Parsen der schemas
        value_schema = fastavro.schema.load_schema(schema_path)

        with open("./data/error_data.json", "r") as file:
            json_ = json.load(file)
        with pytest.raises(ValidationError):
            fastavro.validation.validate(json_, fastavro.parse_schema(json.loads(value_schema_str)))
