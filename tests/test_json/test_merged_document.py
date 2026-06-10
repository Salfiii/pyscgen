import datetime
import json
import os
from typing import Any

from pyscgen.json.merge.merge_documents import DocumentMerger

from json import JSONEncoder

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


def get_result_dict():
    _dict = {
        "simple": {
            "document_list": [
                {
                    "float": 1.23,
                    "int": 10,
                    "string": "text"
                }
            ],
            "float": 1.23,
            "int": 10,
            "mixed_type_element": "text",
            "string": "text",
            "sub_document": {
                "sub_doc_float": 1.23,
                "sub_doc_int": 10,
                "sub_doc_string": "text"
            }
        },
        "complex": {
            "document_list": [
                {
                    "float": 1.23,
                    "float2": 1.23,
                    "int": 10,
                    "string": "text",
                    "string2": "text"
                }
            ],
            "float": 1.23,
            "int": 10,
            "is_null_but_in_every_file": 'text',
            "list_of_doc_lists": [
                [
                    {
                        "a": "text",
                        "b": 10,
                        "c": 1.23,
                        "d": "text"
                    }
                ]
            ],
            "list_of_lists": [
                [
                    "text"
                ]
            ],
            "list_of_lists_int": [
                [
                    10
                ]
            ],
            "mixed_type_element": "text",
            "only_in_4": "text",
            "simple_list": [
                "text"
            ],
            "string": "text",
            "sub_document": {
                "sub_doc_float": 1.23,
                "sub_doc_int": 10,
                "sub_doc_string": "text",
                "sub_document_sub": {
                    "sub_doc_float": 1.23,
                    "sub_doc_int": 10,
                    "sub_sub_doc_string": "text"
                }
            }
        },
        "deeply_nested": {
            "SimpleList": [
                "text"
            ],
            "SntAusFolgemitfortschreibungJN": True,
            "SntBestandsauswertung": {
                "SntBestandsauswertungDynamisch": [
                    {
                        "Attribut": "text",
                        "Datentyp": "text",
                        "Identifikator": "text",
                        "WertText": "text"
                    }
                ],
                "SntID": "text",
                "Versicherungsnummer": 10
            },
            "SntFOVersion": "text"
        },
        "array": {
            "BoolField": True,
            "DocListExistsOnlyIn1": [
                {
                    "a": "text",
                    "b": "text",
                    "c": "text"
                }
            ],
            "DocOnlyIn2": {
                "Attribute": "text",
                "Datatype": "text",
                "ID": "text",
                "NestedDocOnlyIn2": {
                    "Attribute": "text",
                    "Datatype": "text"
                },
                "Value": "text"
            },
            "ListExistsOnlyIn1": [
                "text"
            ],
            "OnlyIn2": "text",
            "Version": "text"
        },
        "nullable_array_and_record": {
            "DocListExistsOnlyIn1": [
                {
                    "a": "text",
                    "b": "text",
                    "c": "text"
                }
            ],
            "DocOnlyIn2": {
                "Attribute": "text",
                "Datatype": "text",
                "ID": "text"
            }
        },
        "nested_array": {
            "list_of_doc_lists": [
                [
                    {
                        "a": "text",
                        "b": 10,
                        "c": 1.23,
                        "d": "text"
                    }
                ]
            ],
            "list_of_lists": [
                [
                    "text"
                ]
            ]
        },
        "nested_doc": {
            "DocOnlyIn2": {
                "Attribute": "text",
                "NestedDocOnlyIn2": {
                    "Datatype": "text"
                }
            },
            "ValueIn2": "text"
        }
    }
    return _dict


def get_data(test: str) -> [dict]:
    """
    Return the JSON-Data from file.
    :return:
    """
    data = []
    test_path: str = os.path.join(get_data_path(), test)
    for filename in os.listdir(test_path):
        file_path = os.path.join(test_path, filename)
        with open(file_path, "r") as file:
            json_ = json.load(file)
            data.append(json_)
    return data


def get_merged_instance():
    return DocumentMerger()

def run_merger_test(test_group_name: str):
    """
    Run a standard merger test: merge documents, write output file, and assert result.
    :param test: Test group name (used for file paths)
    """
    json_merger = get_merged_instance()
    docs: [dict] = get_data(test_group_name)
    merged = json_merger.get_merged_document(docs)

    output_path = get_path_rel_to(__file__, "./out/" + test_group_name + "_merged.json")
    with open(output_path, "w+") as file:
        json.dump(merged, file, indent=4, cls=JSONEncoder)

    assert isinstance(merged, dict)
    assert get_result_dict()[test_group_name] == merged

class TestJSONMerger:

    def test_JSONMerger_simple(self):
        run_merger_test("simple")

    def test_JSONMerger_nested_doc(self):
        run_merger_test("nested_doc")

    def test_JSONMerger_nested_array(self):
        run_merger_test("nested_array")

    def test_JSONMerger_nullable_array_and_record(self):
        run_merger_test("nullable_array_and_record")

    def test_JSONMerger_array(self):
        run_merger_test("array")

    def test_JSONMerger_complex(self):
        run_merger_test("complex")

    def test_JSONMerger_deeply_nested(self):
        run_merger_test("deeply_nested")
