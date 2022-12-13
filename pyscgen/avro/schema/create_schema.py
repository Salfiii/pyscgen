import traceback
import warnings
from typing import Any, Union, List, NoReturn

from pyscgen.json.analyze.analyze_documents import JSONAnalyzer
from pyscgen.json._model.analyze_model import ColumnInfo
from pyscgen.avro._model.record_model import Field, Array, Schema, Record


class AvroSchemaGenerator:

    def __init__(self, alphabetically_ordered_by_path: bool = False, debug: bool = False):
        """

        :param alphabetically_ordered_by_path:
            If true, outputs the fields in the AVRO-Schema alphabetically ordered by name.
            This comes in handy if you need a more deterministic solution.
        :param debug: If True, Debug inf is printed out.
            Defaults to False.
        """
        self.json_analyzer = JSONAnalyzer(alphabetically_ordered_by_path=alphabetically_ordered_by_path)
        self.debug = debug
        self.__list_symbol_extended: str = self.json_analyzer.path_concat_separator + self.json_analyzer.list_symbol

    def create_schema(self, docs: [dict], name: str = "PyScGenClass", namespace: str = "com.pyscgen.avro") -> Schema:
        """
        Create an AVRO-Schema based on the given JSON-Documents/Dicts.
        Internally, the json analyzer and merger are used which is present in this package to gather all needed
        information and create a merged Document on which the AVRO-Schema will be based
        :param docs: List of dicts on which data the AVRO-Schema will be based on
        :param name: Name of the AVRO-Schema used in the "name" Attribute
        :param namespace: AVRO Namespace.
        :return:
        """
        avro_schema: Schema = Schema(name=name, namespace=namespace)
        _, column_infos, _, _, _ = self.json_analyzer.analyze(docs=docs)
        column_info: ColumnInfo
        # loop over the analyzed column infos
        for column_info in column_infos.column_infos:
            # find out if the current column is of type list or dict
            is_list: bool = column_info.data_type_config.python_type == list
            is_dict: bool = column_info.data_type_config.python_type == dict
            has_parent: bool = True if column_info.parent_config else False
            if has_parent:
                parent_is_list: bool = column_info.parent_config.data_type_config.python_type == list
                parent_is_dict: bool = column_info.parent_config.data_type_config.python_type == dict
            else:
                parent_is_list: bool = False
                parent_is_dict: bool = False
            if self.debug:
                print("config: " + str({"path": column_info.path, "is_list": is_list, "is_dict": is_dict,
                                        "parent_is_list": parent_is_list, "parent_is_dict": parent_is_dict}))
            avro_schema = self.__add_element(avro_schema, column_info, is_list, is_dict, has_parent, parent_is_list,
                                             parent_is_dict)

        return avro_schema

    def __add_element(self, avro_schema: Any, column_info: ColumnInfo, is_list: bool, is_dict: bool, has_parent: bool,
                      parent_is_list: bool, parent_is_dict: bool
                      ) -> NoReturn:
        """
        Add a new element to the avro schema based on the given column infos
        :param avro_schema: current avro schema where the new element should be appended
        :param column_info: column info object of the current column which should be appended to the schema
        :param is_list: True if the column is of type list
        :param is_dict: True if the column is of type dict
        :return:
        """

        # use the datatype infos to append the correct value to the element
        if not has_parent:
            field: Field = self.__get_avro_element(column_info, is_list, is_dict, parent_is_list, parent_is_dict)
            avro_schema.add_item(field)
            if self.debug:
                print(avro_schema)
        else:
            self.__assign_wrapper(avro_schema, column_info, is_list, is_dict, parent_is_list, parent_is_dict)
        return avro_schema

    def __assign_wrapper(self, avro_schema: Any,
                         column_info: ColumnInfo,
                         is_list: bool,
                         is_dict: bool,
                         parent_is_list: bool,
                         parent_is_dict: bool,
                         skip_errors: bool = False):
        """
        Creat
        :param avro_schema:
        :param column_info:
        :param is_list:
        :param is_dict:
        :param skip_errors:
        :return:
        """
        path = column_info.parent_config.avro_path
        path_elements_list: list = path.split(self.json_analyzer.path_concat_separator)
        avro_element = self.__get_avro_element(column_info, is_list, is_dict, parent_is_list, parent_is_dict)
        avro_schema_ref = avro_schema
        try:
            for element in path_elements_list:
                if self.debug:
                    print("working on element:" + element)
                # if element == self.json_analyzer.list_symbol:
                # save an avro schema reference before search
                # if the search returns no result, reset for the next search.
                # This helps to iterate nested arrays because they don´t have a name attribute
                avro_schema_ref_safe = avro_schema_ref
                avro_schema_ref = avro_schema_ref.find_child(element)
                if avro_schema_ref is None:
                    avro_schema_ref = avro_schema_ref_safe

            avro_schema_ref.add_item(avro_element)
            if self.debug:
                print("Current AVRO-Schema: " + str(avro_schema.as_dict()))
                print(avro_schema_ref)
        except Exception as e:
            error_info: dict = {"Error": str(e), "Traceback": traceback.format_exc()}
            warnings.warn(str(error_info))
            if not skip_errors:
                raise e
        if self.debug:
            print("avro_schema updated: " + str(avro_schema))
            print("___________________________________________________")

    @staticmethod
    def __get_avro_element(column_info: ColumnInfo, is_list: bool, is_dict: bool,
                           parent_is_list: bool,
                           parent_is_dict: bool, ) -> Union[Field, Record, Array, List[str], str]:
        """
        Create the needed avro element based on the given column info and parent config
        :param column_info: column info object of the current column which should be appended to the schema
        :param is_list: True if the column is of type list
        :param is_dict: True if the column is of type dict
        :param parent_is_list: True if the parent column is of type list
        :param parent_is_dict: True if the parent column is of type dict
        :return:
        """
        name: str = column_info.avro_name
        if is_list:
            if parent_is_list:
                if column_info.has_nulls:
                    array: Array = Array(type=column_info.data_type_config.avro_type, default=[])
                    nullable = ["null", array]
                    return_obj = nullable
                else:
                    array: Array = Array(type=column_info.data_type_config.avro_type)
                    return_obj = array
            else:
                if column_info.has_nulls:
                    array: Array = Array(type=column_info.data_type_config.avro_type)
                    nullable = ["null", array]
                    #@TODO: Is there a usable "default" default?
                    default = None
                    nullability_array = nullable
                else:
                    array: Array = Array(type=column_info.data_type_config.avro_type)
                    default = None
                    nullability_array = array
                return_obj = Field(name=name, type=nullability_array,
                                   logicalType=column_info.data_type_config.avro_logical_type,
                                   **column_info.data_type_config.avro_additional, default=default)
        elif is_dict:
            if parent_is_list:
                record: Record = Record(name=name, type=column_info.data_type_config.avro_type)
                if column_info.has_nulls:
                    nullable = ["null", record]
                    return_obj = nullable
                else:
                    return_obj = record
            else:
                record: Record = Record(name=name, type=column_info.data_type_config.avro_type)
                if column_info.has_nulls:
                    nullable = ["null", record]
                    nullability_record = nullable
                else:
                    nullability_record = record
                return_obj = Field(name=name, type=nullability_record,
                                   logicalType=column_info.data_type_config.avro_logical_type,
                                   **column_info.data_type_config.avro_additional)
        else:
            field_type = ["null",
                          column_info.data_type_config.avro_type] if column_info.has_nulls else column_info.data_type_config.avro_type
            # if its not a dict or list, add the value from the data type config
            if parent_is_list:
                return_obj = field_type
            else:
                return_obj: Field = Field(name=name, type=field_type,
                                          logicalType=column_info.data_type_config.avro_logical_type,
                                          **column_info.data_type_config.avro_additional)

        return return_obj
