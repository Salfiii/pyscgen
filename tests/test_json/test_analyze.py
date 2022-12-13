import datetime
import json

import os

from typing import Any

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


def get_data(test: str) -> [dict]:
    """
    Return the JSON-Data from file.
    :return:
    """
    data_folder: str = "../data/"
    data = []
    test_path: str = data_folder + test
    for filename in os.listdir(test_path):
        file_path = os.path.join(test_path, filename)
        with open(file_path, "r") as file:
            json_ = json.load(file)
            data.append(json_)
    return data


def get_analyzer_instance():
    return JSONAnalyzer()


class TestJSONAnalyzer:

    def test_JSONAnalyzer_simple(self):
        """
        Test the outcome
        :return:
        """
        test = "simple"
        json_analyzer = get_analyzer_instance()
        docs: [dict] = get_data(test)
        collection_data, column_infos, df_flattened, df_dtypes, df_unique = json_analyzer.analyze(docs)
        try:
            with open("./out/" + test + "_column_infos.json", "w+") as file:
                json.dump(json.loads(JSONEncoder().encode(column_infos.as_dict())), file, indent=4)
            with open("./out/" + test + "_collection_data.json", "w+") as file:
                json.dump(json.loads(JSONEncoder().encode(collection_data.as_dict())), file, indent=4)
        except Exception as e:
            print("Something went wrong while trying to write a file: " + str(e))
        selector = [info.name == "mixed_type_element" for info in column_infos.column_infos]
        result = [x for x, y in zip(column_infos.column_infos, selector) if y][0]
        assert result.data_type_config.python_type == str

    def test_JSONAnalyzer_nested_array(self):
        """
        Test the outcome
        :return:
        """
        test = "nested_array"
        json_analyzer = get_analyzer_instance()
        docs: [dict] = get_data(test)
        collection_data, column_infos, df_flattened, df_dtypes, df_unique = json_analyzer.analyze(docs)
        try:
            with open("./out/" + test + "_column_infos.json", "w+") as file:
                json.dump(json.loads(JSONEncoder().encode(column_infos.as_dict())), file, indent=4)
            with open("./out/" + test + "_collection_data.json", "w+") as file:
                json.dump(json.loads(JSONEncoder().encode(collection_data.as_dict())), file, indent=4)
        except Exception as e:
            print("Something went wrong while trying to write a file: " + str(e))
        selector = [info.name == "list_of_doc_lists" for info in column_infos.column_infos]
        result = [x for x, y in zip(column_infos.column_infos, selector) if y][0]
        assert result.data_type_config.python_type == list

    def test_JSONAnalyzer_nested_doc(self):
        """
        Test the outcome
        :return:
        """
        test = "nested_doc"
        json_analyzer = get_analyzer_instance()
        docs: [dict] = get_data(test)
        collection_data, column_infos, df_flattened, df_dtypes, df_unique = json_analyzer.analyze(docs)
        try:
            with open("./out/" + test + "_column_infos.json", "w+") as file:
                json.dump(json.loads(JSONEncoder().encode(column_infos.as_dict())), file, indent=4)
            with open("./out/" + test + "_collection_data.json", "w+") as file:
                json.dump(json.loads(JSONEncoder().encode(collection_data.as_dict())), file, indent=4)
        except Exception as e:
            print("Something went wrong while trying to write a file: " + str(e))
        selector = [info.name == "DocOnlyIn2" for info in column_infos.column_infos]
        result = [x for x, y in zip(column_infos.column_infos, selector) if y][0]
        assert result.data_type_config.python_type == dict

    def test_JSONAnalyzer_nullable_array_and_record(self):
        """
        Test the outcome
        :return:
        """
        test = "nullable_array_and_record"
        json_analyzer = get_analyzer_instance()
        docs: [dict] = get_data(test)
        collection_data, column_infos, df_flattened, df_dtypes, df_unique = json_analyzer.analyze(docs)
        try:
            with open("./out/" + test + "_column_infos.json", "w+") as file:
                json.dump(json.loads(JSONEncoder().encode(column_infos.as_dict())), file, indent=4)
            with open("./out/" + test + "_collection_data.json", "w+") as file:
                json.dump(json.loads(JSONEncoder().encode(collection_data.as_dict())), file, indent=4)
        except Exception as e:
            print("Something went wrong while trying to write a file: " + str(e))
        selector = [info.name == "Attribute" for info in column_infos.column_infos]
        result = [x for x, y in zip(column_infos.column_infos, selector) if y][0]
        assert result.data_type_config.python_type == str

    def test_JSONAnalyzer_array(self):
        """
        Test the outcome
        :return:
        """
        test = "array"
        json_analyzer = get_analyzer_instance()
        docs: [dict] = get_data(test)
        collection_data, column_infos, df_flattened, df_dtypes, df_unique = json_analyzer.analyze(docs)
        try:
            with open("./out/" + test + "_column_infos.json", "w+") as file:
                json.dump(json.loads(JSONEncoder().encode(column_infos.as_dict())), file, indent=4)
            with open("./out/" + test + "_collection_data.json", "w+") as file:
                json.dump(json.loads(JSONEncoder().encode(collection_data.as_dict())), file, indent=4)
        except Exception as e:
            print("Something went wrong while trying to write a file: " + str(e))
        selector = [info.name == "BoolField" for info in column_infos.column_infos]
        result = [x for x, y in zip(column_infos.column_infos, selector) if y][0]
        assert result.data_type_config.python_type == bool

    def test_JSONAnalyzer_complex(self):
        """
        Test the outcome
        :return:
        """
        test = "complex"
        json_analyzer = get_analyzer_instance()
        docs: [dict] = get_data(test)
        collection_data, column_infos, df_flattened, df_dtypes, df_unique = json_analyzer.analyze(docs)
        try:
            with open("./out/" + test + "_column_infos.json", "w+") as file:
                json.dump(json.loads(JSONEncoder().encode(column_infos.as_dict())), file, indent=4)
            with open("./out/" + test + "_collection_data.json", "w+") as file:
                json.dump(json.loads(JSONEncoder().encode(collection_data.as_dict())), file, indent=4)
        except Exception as e:
            print("Something went wrong while trying to write a file: " + str(e))
        selector = [info.name == "mixed_type_element" for info in column_infos.column_infos]
        result = [x for x, y in zip(column_infos.column_infos, selector) if y][0]
        assert result.data_type_config.python_type == str

    def test_JSONAnalyzer_deeply_nested(self):
        """
        Test the outcome
        :return:
        """
        test = "deeply_nested"
        json_analyzer = get_analyzer_instance()
        docs: [dict] = get_data(test)
        collection_data, column_infos, df_flattened, df_dtypes, df_unique = json_analyzer.analyze(docs)
        try:
            with open("./out/" + test + "_column_infos.json", "w+") as file:
                json.dump(json.loads(JSONEncoder().encode(column_infos.as_dict())), file, indent=4)
            with open("./out/" + test + "_collection_data.json", "w+") as file:
                json.dump(json.loads(JSONEncoder().encode(collection_data.as_dict())), file, indent=4)
        except Exception as e:
            print("Something went wrong while trying to write a file: " + str(e))
        selector = [info.name == "SntFOVersion" for info in column_infos.column_infos]
        result = [x for x, y in zip(column_infos.column_infos, selector) if y][0]
        assert result.data_type_config.python_type == str
