import collections.abc as collections
import operator
import typing

import shortuuid
import pandas as pd

from pyscgen.json._model.document_model import Document, Collection, Column
from pyscgen.json._model.analyze_model import ColumnInfo, ColumnInfos, ParentColumnInfo
from pyscgen.__config.dtype_config import DataTypeConfig


class JSONAnalyzer:

    def __init__(self, alphabetically_ordered_by_path: bool = True):
        """

        :param alphabetically_ordered_by_path: If true, returns the analyzed elements ordered alphabetically by path.
            Defaults to True.
        """
        self.datatype_config = DataTypeConfig()
        self.alphabetically_ordered_by_path = alphabetically_ordered_by_path
        self.__list_symbol: str = "0"
        self.__path_concat_separator: str = "."
        self.__dict_path_append_name: str = "_record"
        self.__list_path_append_name: str = "_element"

    @property
    def list_symbol(self) -> str:
        """
        Returns the string which is used to represent list elements in a path
        :return:
        """
        return self.__list_symbol

    @property
    def path_concat_separator(self) -> str:
        """
        Returns the string which is used to separate elements in the path
        :return:
        """
        return self.__path_concat_separator

    def __inspect_lists(self, list_: list, parent_key: str = '') -> (dict, dict):
        """
        Inspect a list recursively.
        :param list_: list which should be inspected
        :param parent_key: if it´s not a root element, a parent key needs to be supplied for path building.
            Defaults to ''
        :return:
        """
        items = []
        dtypes = []
        for element in list_:
            path = parent_key + self.__path_concat_separator + self.__list_symbol
            if isinstance(element, collections.MutableMapping):
                items.append((path, None))
                dtypes.append((path, type(element)))
                recursive_data = self.__get_values_and_types(element, parent_key=path)
                items.extend(recursive_data[0].items())
                dtypes.extend(recursive_data[1].items())
            else:
                if isinstance(element, list):
                    items.append((path, None))
                    dtypes.append((path, type(element)))
                    recursive_data = self.__inspect_lists(list_=element, parent_key=path)
                    items.extend(recursive_data[0].items())
                    dtypes.extend(recursive_data[1].items())
                else:
                    items.append((path, element))
                    dtypes.append((path, type(element)))
        items_hashable = self.__remove_unhashable(items)
        items_hashable = dict(list(set(items_hashable)))
        dtypes = dict(list(set(dtypes)))
        return items_hashable, dtypes

    def __get_values_and_types(self, doc: dict, parent_key: str = '') -> (dict, dict):
        """
        Inspect a document recursively to get the values and types for each element.
        Hints taken from: https://stackoverflow.com/questions/6027558/flatten-nested-dictionaries-compressing-keys
        :param doc: input document to get the values and types from.
        :param parent_key: parent key - keep at default value, only needed for recursive calls.
        :return:
        Returns a tuple with tho dictionaries:

        - 1.: {key: value}: returns the key with the data present under it.
        - 2.: {key: type}: returns the key with the associated type.
        """
        items = []
        dtypes = []
        for k, v in doc.items():
            path = parent_key + self.__path_concat_separator + k if parent_key else k
            if isinstance(v, collections.MutableMapping):
                if k:
                    # @TODO: Here, fix dict appended to items
                    path = parent_key + self.__path_concat_separator + k if parent_key != "" else k
                    items.append((path, v))
                    dtypes.append((path, type(v)))
                recursive_data = self.__get_values_and_types(v, path)
                items.extend(recursive_data[0].items())
                dtypes.extend(recursive_data[1].items())
            else:
                if isinstance(v, list):
                    path = parent_key + self.__path_concat_separator + k if parent_key else k
                    items.append((path, v))
                    dtypes.append((path, type(v)))
                    recursive_data = self.__inspect_lists(list_=v, parent_key=path)
                    items.extend(recursive_data[0].items())
                    dtypes.extend(recursive_data[1].items())
                else:
                    items.append((path, v))
                    dtypes.append((path, type(v)))
        return dict(items), dict(dtypes)

    def __get_values_and_types_formatted(self, doc: dict) -> (Document, dict, dict):
        """
        Puts the value and type information from __get_values_and_types into a Column object and afterwards those
        into an Document object
        :param doc: input document to get the infos from
        :return:
        Returns a tuple with a Document-Object and two dictionaries:

        - 1.: Document Object
        - 2.: {key: value}: returns the key with the data present under it.
        - 3.: {key: type}: returns the key with the associated type.
        """
        items, dtypes = self.__get_values_and_types(doc=doc)
        column_count = len(items)
        column_info_list = []
        for k, v in items.items():
            column_info = Column(
                name=k,
                path=k,
                is_null=False,
                data_type=dtypes[k],
                value=v
            )
            column_info_list.append(column_info)
        document_info = Document(
            columns=column_info_list,
            size=column_count
        )
        return document_info, items, dtypes

    def __get_unique_column_values(self, df_flattened: pd.DataFrame, df_dtypes: pd.DataFrame) -> (ColumnInfos, list, list):
        """
        Get unique columns values based on the DataFrame input
        :param df_flattened: data frame which holds the flattened json
        :param df_dtypes:
        :return:
        """

        columns: list = []
        data: list = []
        column_infos_list: list = []
        column_path: str = None
        for column_path in df_flattened:
            try:
                values = df_flattened[column_path].unique().tolist()
            except Exception as e:
                # @TODO: Generic solution for dicts and lists of lists
                merged_ls = []
                for index, value in df_flattened[column_path].iteritems():
                    merged_ls.append(value)
                # https://stackoverflow.com/questions/3724551/python-uniqueness-for-list-of-lists
                try:
                    values = [list(x) for x in set(tuple(x) for x in merged_ls)]
                except Exception as e:
                    values = df_flattened[column_path].tolist()
            values_without_nan = [x for x in values if str(x) != 'nan' and x is not None]
            values_extended_for_df_without_nan = [column_path] + values_without_nan
            len_values_without_nan = len(values_without_nan)
            len_values = len(values)
            has_none = len_values_without_nan != len_values
            data_types_list = df_dtypes[column_path].dropna().unique().tolist()
            parent_info = None
            if column_path.count(self.path_concat_separator) >= 1:
                parent_col_name_split: list = column_path.rsplit(self.path_concat_separator, 1)
                parent_column_path: str = parent_col_name_split[0]
                parent_column_avro_path: str = self.__create_avro_path(parent_column_path, self.__path_concat_separator, self.__list_symbol)
                parent_column_name: str = parent_column_path.rsplit(self.path_concat_separator, 1)[-1]
                parent_column_avro_name: str = parent_column_avro_path.rsplit(self.path_concat_separator, 1)[-1]
                parent_data_types_list = df_dtypes[parent_column_path].dropna().unique().tolist()
                parent_info = ParentColumnInfo(
                    name=parent_column_name,
                    path=parent_column_path,
                    data_type_config=self.datatype_config.choose_type_info(parent_data_types_list),
                    avro_name = parent_column_avro_name,
                    avro_path = parent_column_avro_path
                )
            column_avro_path: str = self.__create_avro_path(column_path, self.__path_concat_separator, self.__list_symbol)
            column_avro_name: str = column_avro_path.rsplit('.', 1)[-1]
            column_name: str = column_path.rsplit('.', 1)[-1]
            column_info = ColumnInfo(
                name=column_name,
                path=column_path,
                has_nulls=has_none,
                density=len_values_without_nan / len_values if len_values != 0 else 0,
                unique_values=values_without_nan,
                data_types=data_types_list,
                mixed_types=len(data_types_list) > 1,
                data_type_config=self.datatype_config.choose_type_info(data_types_list),
                parent_config=parent_info,
                avro_name=column_avro_name,
                avro_path=column_avro_path
            )
            column_infos_list.append(column_info)

            if values:
                data.append(values_extended_for_df_without_nan)
                columns.append(column_path)

        # sort the list by the path attribute of the column_info class
        if self.alphabetically_ordered_by_path:
            column_infos_list = sorted(column_infos_list, key=operator.attrgetter('path'))
        column_infos = ColumnInfos(
            column_infos=column_infos_list
        )
        return column_infos, columns, data

    def __create_avro_path(self, path: str, split_string: str, search_string: str):
        """
        replaces the search string withing the path with an avro compliant "record"-name
        :param path: input path
        :param split_string: string which can be used to split the path in
        :param search_string: string which should be searched and altered
        :return:
        """
        container = []
        path_split: [str] = path.split(split_string)
        for i, element in enumerate(path_split):
            if element == search_string:
                container.append(container[i - 1] + self.__dict_path_append_name )
            else:
                container.append(element)
        avro_path = ".".join(container)
        return avro_path

    def __remove_unhashable(self, tuple_list: typing.List[typing.Tuple]):
        """
        Remove all non-hashable elements from the tuple list and replace it with None
        This is a bug fix because I didn´t find a quick solution why sometimes a dict slips through the logic.
        It works, so why bother?!
        :param tuple_list:
        :return:
        """
        return_tuple_list: list = []
        for i, value in enumerate(tuple_list):
            if not isinstance(value[1], collections.Hashable):
                new_tuple = (value[0], None)
                return_tuple_list.append(new_tuple)
            else:
                return_tuple_list.append(value)
        return return_tuple_list

    def analyze(self, docs: [dict]) -> typing.Tuple[Collection, ColumnInfos, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        """
        Analyze a list of dictionaries/json documents to get all infos about the structure, nullability etc.
        :param docs: list of dictionaries which should be analyzed
        :return: Tuple of 5 elements, listed below.
            1.: Collection: Infos about each document, columns present in it, if it is null, the data type etc.
            2.: ColumnInfos: Main Output of the analyze function. Contains all infos to all found columns within all
                json documents with infos like: name, path, has_nulls, density, unique_values etc.
            3.: pandas DataFrame which holds the flattened json documents.
            4.: pandas DataFrame which holds information about which dtypes are found in a column in the given documents.
            5.: pandas DataFrame which holds information about unique values of each flattened column of the given json documents.

        """
        document_data_list: list = []
        values: list = []
        data_types: list = []
        doc: dict
        for i, doc in enumerate(docs):
            document_info, items, dtypes = self.__get_values_and_types_formatted(doc)
            document_data_list.append(document_info)
            values.append(items)
            data_types.append(dtypes)
        collection_data: Collection = Collection(
            documents=document_data_list,
            size=i
        )
        df_flattened = pd.DataFrame(values)
        df_dtypes = pd.DataFrame(data_types)
        column_infos, columns, data = self.__get_unique_column_values(df_flattened, df_dtypes)
        df_unique = pd.DataFrame(data=data)
        return collection_data, column_infos, df_flattened, df_dtypes, df_unique
