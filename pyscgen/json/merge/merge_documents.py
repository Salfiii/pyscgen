import warnings
from typing import Any

from glom import assign, Assign, glom

from pyscgen.json.analyze.analyze_documents import JSONAnalyzer
from pyscgen.json._model.analyze_model import ColumnInfo


class DocumentMerger:

    def __init__(self):
        """

        """
        self.json_analyzer = JSONAnalyzer()
        self.__list_symbol_extended: str = self.json_analyzer.path_concat_separator + self.json_analyzer.list_symbol

    def get_merged_document(self, docs: [dict]) -> dict:
        """
        Create a single representation out of n supplied JSON-Documents/dicts.
        All found columns are merged into one document.
        Please note that the output document contains only placeholder values for each key with the correct datatype.
        Internally, the json analyzer is used which is present in this package to gather all needed information.
        :param docs: List of JSON-Documents/Dicts which should be analyzed and merged into one.
        :return:
        """
        _ , column_infos, _, _, _ = self.json_analyzer.analyze(docs=docs)
        merged: dict = {}
        column_info: ColumnInfo
        # loop over the analyzed column infos
        for column_info in column_infos.column_infos:
            # find out if the current column is of type list or dict
            is_list: bool = column_info.data_type_config.python_type == list
            is_dict: bool = column_info.data_type_config.python_type == dict
            # add a new element to the merged dict based on the gathered infos
            merged = self.__add_element(merged, column_info, is_list, is_dict)
        return merged

    def __add_element(self, merged: dict, column_info: ColumnInfo, is_list: bool, is_dict: bool):
        """
        Adds an element to merged based on the given infos.
        :param merged: merged dict which is iteratively built
        :param column_info: colum info object
        :param is_list: True if the current column is a list
        :param is_dict: True if the current column is a dict
        :return:
        """

        path: str = column_info.path
        # use the datatype infos to append the correct value to the element
        if is_list:
            # an empty list is not working because your don´t have an element [0] there
            # Also None is not working too because you can´t assign to it. That´s why I´m using Any.
            _ = self.__assign_wrapper(merged, path, [Any], is_list, is_dict)
        elif is_dict:
            _ = self.__assign_wrapper(merged, path, {}, is_list, is_dict)
        else:
            # if its not a dict or list, add the value from the data type config
            _ = self.__assign_wrapper(merged, path, column_info.data_type_config.python_value,
                                      is_list, is_dict)
        return merged

    def __assign_wrapper(self, obj: Any,
                         path: str,
                         value: Any,
                         is_list: bool,
                         is_dict: bool,
                         skip_errors: bool = True):
        """
        A wrapper function around gloms assign method for error handling and building T[]-Paths.
        :param obj: object to assign to.
        :param path: assignment path
        :param value: value to assign
        :param is_list: True if the current column is a list
            @DEPRECATED
        :param is_dict: True if the current column is a dict
            @DEPRECATED
        :param skip_errors: True if errors should be skipped and not raised.
            Defaults to True.
        :return:
        """
        path_elements_list: list = path.split(self.json_analyzer.path_concat_separator)
        missing_filler = None
        t_ref = None

        def missing_filler_callable():
            """
            To be usable in the missing-parameter of glom.Assign it needs to be a callable.
            So I just wrapped the returning of the needed value in a simple function.
            :return:
            """
            return missing_filler

        # simply try to use basic glom.assign to assign the given value to the merged dict.
        try:
            _ = assign(obj, path, value)
        # If glom.assign is not working - e.g. for 0 in the past which indicates a list then build
        # the T[]-assignment by hand iteratively based on the given path.
        except Exception as e:
            try:
                for element in path_elements_list:
                    if element == self.json_analyzer.list_symbol:
                        if t_ref is None:
                            t_ref = "T[0]"
                        else:
                            t_ref = t_ref + "[0]"
                    else:
                        if t_ref is None:
                            t_ref = "T['" + element + "']"
                        else:
                            t_ref = t_ref + "['" + element + "']"
                t_ref = eval(t_ref)
                _ = glom(obj, Assign(t_ref, value, missing=missing_filler_callable))
            except Exception as e:
                warnings.warn(str(e))
                if not skip_errors:
                    raise e
