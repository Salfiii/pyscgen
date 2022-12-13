import datetime
import decimal
import warnings
from typing import Any

from pyscgen.json._model.datatype_model import DataTypeConfigModel


class DataTypeConfig:

    class ContainsNotAllNeededConfigEntriesException(Exception):
        """
        Exception to display if the config is altered and not all needed entries - for each datatype -  are set
        """

        def __init__(self, needed_entries: [str]):
            self.message = "Please provide at least config for the following dtypes: " + str(needed_entries)
            super().__init__(self.message)

    def __init__(self):
        """

        """
        self.default_data_type: str = "str"
        self.none_data_type: str = "NoneType"
        self.__data_type_config: dict = {
            self.none_data_type: {
                "python_type": type(None),
                "python_value": None,
                "avro_type": "null",
                "avro_logical_type": None,
                "json_type": "null"
            },
            "bool": {
                "python_type": bool,
                "python_value": True,
                "avro_type": "boolean",
                "avro_logical_type": None,
                "json_type": "boolean"
            },
            "int": {
                "python_type": int,
                "python_value": int(10),
                "avro_type": "int",
                "avro_logical_type": None,
                "json_type": "integer"
            },
            "float": {
                "python_type": float,
                "python_value": 1.23,
                "avro_type": "float",
                "avro_logical_type": None,
                "json_type": "number"
            },
            "bytes": {
                "python_type": bytes,
                "python_value": b'ByteString',
                "avro_type": "bytes",
                "avro_logical_type": None,
                "json_type": "string"
            },
            "str": {
                "python_type": str,
                "python_value": "text",
                "avro_type": "string",
                "avro_logical_type": None,
                "json_type": "string"
            },
            "dict": {
                "python_type": dict,
                "python_value": {},
                "avro_type": "record",
                "avro_logical_type": None,
                "json_type": "object"
            },
            "list": {
                "python_type": list,
                "python_value": [Any],
                "avro_type": "array",
                "avro_logical_type": None,
                "json_type": "array"
            },
            "decimal": {
                "python_type": type(decimal.Decimal(1)),
                "python_value": decimal.Decimal(1),
                "avro_type": "bytes",
                "avro_logical_type": "decimal",
                "avro_additional": {"precision": 4,
                                    "scale": 2
                                    },
                "json_type": None
            },
            "datetime.date": {
                "python_type": datetime.date,
                "python_value": datetime.date.today(),
                "avro_type": "int",
                "avro_logical_type": "date",
                "json_type": None
            },
            "datetime.time": {
                "python_type": datetime.time,
                "python_value": datetime.datetime.now().time(),
                "avro_type": "int",
                "avro_logical_type": "time-millis",
                "json_type": None
            },
            "datetime.datetime": {
                "python_type": datetime.datetime,
                "python_value": datetime.datetime.now(),
                "avro_type": "long",
                "avro_logical_type": "timestamp-millis",
                "json_type": None
            },
            "datetime.timedelta": {
                "python_type": datetime.timedelta,
                "python_value": datetime.timedelta(
                    days=50,
                    seconds=27,
                    microseconds=10,
                    milliseconds=29000,
                    minutes=5,
                    hours=8,
                    weeks=2
                ),
                "avro_type": "fixed",
                "avro_logical_type": "duration",
                "avro_additional": {"size": 12},
                "json_type": None
            },

        }

    @property
    def data_type_config(self):
        """
        Get the data_type_config
        :return:
        """
        return self.__data_type_config

    @data_type_config.setter
    def data_type_config(self, data_type_config: dict):
        """
        Set the data type config
        :param data_type_config:
        :return:
        """
        old_keys: set = set(self.__data_type_config.copy().keys())
        new_keys: set = set(data_type_config.copy().keys())
        contains_all_old_keys: bool = new_keys.issuperset(old_keys)
        if contains_all_old_keys:
            self.__data_type_config = data_type_config
        else:
            raise self.ContainsNotAllNeededConfigEntriesException(list(old_keys))

    @staticmethod
    def get_clean_type_string(data_type: str) -> str:
        """
        Pass a data_type from the config and get a clean type string stripped of <class> etc.
        :param data_type:
        :return:
        """
        data_type_cleaned: str = data_type.replace("<class '", "").replace("'>", "").strip().lower()
        return data_type_cleaned

    def get_type_info_by_name(self, data_type: str) -> DataTypeConfigModel:
        """
        Get the matching type info according to the string representation of the python type.
        The given type is striped and converted to lowercase to always match.
        If no matching type is found, "str" is returned as default.
        :param data_type:
        :return:
        """
        type_lower_trim: str = self.get_clean_type_string(data_type)
        try:
            type_config: dict = self.data_type_config[type_lower_trim]
        except Exception as e:
            warnings.warn(
                "The given type '" + data_type + "' was not present in the data type config, 'str' is returned as default.")
            type_config: dict = self.data_type_config[self.default_data_type]
        return DataTypeConfigModel(**type_config)

    def choose_type_info(self, data_types: []) -> DataTypeConfigModel:
        """
        Pass in a list of datatypes and get back the matchin one.
        Basically, if the list contains multiple datatypes, the default_data_type is returned which usually is string.
        Otherwise, the magic DataTypeConfig is extracted and returned.
        :param data_types:
        :return:
        """
        data_type_config: DataTypeConfigModel
        if len(data_types) == 2:
            for i, data_type in enumerate(data_types):
                data_type = str(data_type)
                cleaned_type: str = self.get_clean_type_string(data_type).lower()
                if cleaned_type == self.none_data_type.lower():
                    if i == 1:
                        selector = 0
                    else:
                        selector = 1
                    data_type_str: str = str(data_types[selector])
                    data_type_config = self.get_type_info_by_name(data_type_str)
                else:
                    data_type_config = self.get_type_info_by_name(self.default_data_type)
        elif data_types is None:
            data_type_config = self.get_type_info_by_name(self.default_data_type)
        else:
            data_type_str: str = str(data_types[0])
            data_type_config = self.get_type_info_by_name(data_type_str)
        return data_type_config
