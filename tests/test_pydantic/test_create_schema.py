import datetime
import decimal
import glob
import os
import json

import fastavro
import pytest

from pyscgen.pydantic.schema.create_schema import PydanticSchemaGenerator


def get_test_avro_schema():
    """

    :return:
    """
    file_path: str = "./in/test_schema.avsc"
    with open(file_path, "r") as file:
        avro_str = file.read()
    return avro_str


def get_data(test: str) -> dict:
    """
    Return the JSON-Data from file.
    :return:
    """
    return_obj: dict = {}
    data_folder: str = "../data/"
    data = []
    test_path: str = data_folder + test
    for filename in os.listdir(test_path):
        file_path = os.path.join(test_path, filename)
        with open(file_path, "r") as file:
            json_ = json.load(file)
            data.append(json_)
    return_obj[test] = data
    data = None
    return return_obj


def get_instance():
    return PydanticSchemaGenerator(True, False)


class TestPydanticSchemaGenerator:

    def test_schema_generation_simple(self):
        generator = get_instance()
        data: dict = get_data("simple")
        for test, docs in data.items():
            print("working on test: " + test)
            schema = generator.create_schema(docs)
            schema_path: str = "./out/" + test + "_pydantic_model.py"
            with open(schema_path, "w+") as file:
                file.write(schema)


    def test_schema_generation_nested_doc(self):
        generator = get_instance()
        data: dict = get_data("nested_doc")
        for test, docs in data.items():
            print("working on test: " + test)
            schema = generator.create_schema(docs)
            schema_path: str = "./out/" + test + "_pydantic_model.py"
            with open(schema_path, "w+") as file:
                file.write(schema)

    def test_schema_generation_nested_array(self):
        generator = get_instance()
        data: dict = get_data("nested_array")
        for test, docs in data.items():
            print("working on test: " + test)
            schema = generator.create_schema(docs)
            schema_path: str = "./out/" + test + "_pydantic_model.py"
            with open(schema_path, "w+") as file:
                file.write(schema)

    def test_schema_generation_nullable_array_and_record(self):
        generator = get_instance()
        data: dict = get_data("nullable_array_and_record")
        for test, docs in data.items():
            print("working on test: " + test)
            schema = generator.create_schema(docs)
            schema_path: str = "./out/" + test + "_pydantic_model.py"
            with open(schema_path, "w+") as file:
                file.write(schema)

    def test_schema_generation_complex(self):
        generator = get_instance()
        data: dict = get_data("complex")
        for test, docs in data.items():
            print("working on test: " + test)
            schema = generator.create_schema(docs)
            schema_path: str = "./out/" + test + "_pydantic_model.py"
            with open(schema_path, "w+") as file:
                file.write(schema)

    def test_schema_generation_deeply_nested(self):
        generator = get_instance()
        data: dict = get_data("deeply_nested")
        for test, docs in data.items():
            print("working on test: " + test)
            schema = generator.create_schema(docs)
            schema_path: str = "./out/" + test + "_pydantic_model.py"
            with open(schema_path, "w+") as file:
                file.write(schema)

    def test_schema_generation_array(self):
        generator = get_instance()
        data: dict = get_data("array")
        for test, docs in data.items():
            print("working on test: " + test)
            schema = generator.create_schema(docs)
            schema_path: str = "./out/" + test + "_pydantic_model.py"
            with open(schema_path, "w+") as file:
                file.write(schema)

    def test_schema_generation_northdata(self):
        generator = get_instance()
        data: dict = get_data("northdata")
        for test, docs in data.items():
            print("working on test: " + test)
            schema = generator.create_schema(docs)
            schema_path: str = "./out/" + test + "_pydantic_model.py"
            with open(schema_path, "w+") as file:
                file.write(schema)

    def test_schema_generator_all_dtypes(self):
        generator = get_instance()
        test = "all_types"
        data: [dict] = {
            "none": None,
            "bool": True,
            "int": 10,
            "float": 1.23,
            "bytes": b'ByteString',
            "str": "string",
            "dict": {"string": "str"},
            "list": ["str"],
            "decimal": decimal.Decimal(1),
            "datetime_date": datetime.date.today(),
            "datetime_time": datetime.datetime.now().time(),
            "datetime_datetime": datetime.datetime.now(),
            "datetime_timedelta": datetime.timedelta(
                days=50,
                seconds=27,
                microseconds=10,
                milliseconds=29000,
                minutes=5,
                hours=8,
                weeks=2
            )
        }
        # bytes is currently not supported to converto from avro to pydantic
        with pytest.raises(NotImplementedError):
            schema = generator.create_schema([data])
            schema_path: str = "./out/" + test + "_pydantic_model.py"
            with open(schema_path, "w+") as file:
                file.write(schema)



