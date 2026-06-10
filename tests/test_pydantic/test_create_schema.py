import datetime
import decimal
import glob
import os
import json

import fastavro
import pytest

from pyscgen.pydantic.schema.create_schema import PydanticSchemaGenerator
from tests.test_data_commons import get_data_path, get_path_rel_to, get_file_folder

def get_test_avro_schema():
    """

    :return:
    """
    file_path: str = get_path_rel_to(__file__, "./in/test_schema.avsc")
    with open(file_path, "r") as file:
        avro_str = file.read()
    return avro_str


def get_data(test: str) -> dict:
    """
    Return the JSON-Data from file.
    :return:
    """
    return_obj: dict = {}
    data = []
    test_path: str = os.path.join(get_data_path(), test)
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

def run_schema_test(test_group_name: str):
    """
    Run a standard schema test.
    :param test_group_name: Test group name (used for file paths)
    """
    generator = get_instance()
    data: dict = get_data(test_group_name)

    for test, docs in data.items():
        print("working on test: " + test)
        schema = generator.create_schema(docs)
        schema_str = str(schema)
        schema_path: str = get_path_rel_to(__file__,  "./out/" + test + "_out_schema.avsc")
        with open(schema_path, "w+") as file:
            file.write(schema.__str__())
        print(schema_str)
        # Einlesen des ersten gefundenen AVRO-Schemas
        with open(schema_path) as value_schema_file:
            value_schema_str = value_schema_file.read()

class TestPydanticSchemaGenerator:

    def test_schema_generation_simple(self):
        run_schema_test("simple")

    def test_schema_generation_nested_doc(self):
        run_schema_test("nested_doc")


    def test_schema_generation_nested_array(self):
        run_schema_test("nested_array")


    def test_schema_generation_nullable_array_and_record(self):
        run_schema_test("nullable_array_and_record")


    def test_schema_generation_complex(self):
        run_schema_test("complex")


    def test_schema_generation_deeply_nested(self):
        run_schema_test("deeply_nested")


    def test_schema_generation_array(self):
        run_schema_test("array")

    def test_schema_generation_northdata(self):
        run_schema_test("northdata")

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



