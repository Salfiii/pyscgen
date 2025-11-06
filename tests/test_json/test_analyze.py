import datetime
import json
import os
from typing import Any

from pyscgen.json.analyze.analyze_documents import JSONAnalyzer
from tests.test_data_commons import get_data_path, get_path_rel_to


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


def get_data(test: str) -> [dict]:
    """
    Return the JSON-Data from file.
    :return:
    """
    data = []
    test_path: str = os.path.join(get_data_path(), test)
    for filename in os.listdir(test_path):
        file_path = os.path.join(test_path, filename)
        with open(file_path) as file:
            json_ = json.load(file)
            data.append(json_)
    return data


def get_analyzer_instance():
    return JSONAnalyzer()


def run_analyzer_test(test: str, field_name: str, expected_type: type):
    """
    Run a standard analyzer test: analyze documents, write output files, and assert field type.
    :param test: Test name (used for file paths)
    :param field_name: Name of the field to check
    :param expected_type: Expected Python type for the field
    """
    json_analyzer = get_analyzer_instance()
    docs: [dict] = get_data(test)
    collection_data, column_infos, df_flattened, df_dtypes, df_unique = json_analyzer.analyze(docs)
    
    try:
        column_infos_path = get_path_rel_to(__file__, "./out/" + test + "_column_infos.json")
        with open(column_infos_path, "w+") as file:
            json.dump(json.loads(JSONEncoder().encode(column_infos.as_dict())), file, indent=4)
        
        collection_data_path = get_path_rel_to(__file__, "./out/" + test + "_collection_data.json")
        with open(collection_data_path, "w+") as file:
            json.dump(json.loads(JSONEncoder().encode(collection_data.as_dict())), file, indent=4)
    except Exception as e:
        print("Something went wrong while trying to write a file: " + str(e))
    
    selector = [info.name == field_name for info in column_infos.column_infos]
    result = [x for x, y in zip(column_infos.column_infos, selector) if y][0]
    assert result.data_type_config.python_type == expected_type


class TestJSONAnalyzer:

    def test_JSONAnalyzer_simple(self):
        run_analyzer_test("simple", "mixed_type_element", str)

    def test_JSONAnalyzer_nested_array(self):
        run_analyzer_test("nested_array", "list_of_doc_lists", list)

    def test_JSONAnalyzer_nested_doc(self):
        run_analyzer_test("nested_doc", "DocOnlyIn2", dict)

    def test_JSONAnalyzer_nullable_array_and_record(self):
        run_analyzer_test("nullable_array_and_record", "Attribute", str)

    def test_JSONAnalyzer_array(self):
        run_analyzer_test("array", "BoolField", bool)

    def test_JSONAnalyzer_complex(self):
        run_analyzer_test("complex", "mixed_type_element", str)

    def test_JSONAnalyzer_deeply_nested(self):
        run_analyzer_test("deeply_nested", "SntFOVersion", str)
