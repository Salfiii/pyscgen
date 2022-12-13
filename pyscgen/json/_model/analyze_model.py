from typing import List, Optional
from dataclasses import asdict
from pydantic.dataclasses import dataclass

from pyscgen.json._model.__model_config import ModelConfig
from pyscgen.json._model.datatype_model import DataTypeConfigModel


@dataclass(config=ModelConfig)
class ParentColumnInfo:
    name: str
    path: str
    avro_path: str
    avro_name: str
    data_type_config: Optional[DataTypeConfigModel] = None

    def __init__(self, name: str,
                 path: str,
                 avro_path: str,
                 avro_name: str,
                 data_type_config: Optional[DataTypeConfigModel] = None):
        """
        Information about the parent column of the current Colum
        :param name: Name of the parent column
        :param path: JSON Path to access the parent column
        :param avro_path: AVRO Path
        :param avro_name:  AVRO Name
        :param data_type_config: DataTypeConfigModel object
        """
        self.name = name
        self.path = path
        self.avro_name = avro_name
        self.avro_path = avro_path
        self.data_type_config = data_type_config


@dataclass(config=ModelConfig)
class ColumnInfo:
    name: str
    path: str
    has_nulls: bool
    density: float
    unique_values: List[object]
    data_types: List[type]
    mixed_types: bool
    avro_path: str
    avro_name: str
    data_type_config: Optional[DataTypeConfigModel] = None
    parent_config: Optional[ParentColumnInfo] = None

    def __init__(self, name: str,
                 path: str,
                 has_nulls: bool,
                 density: float,
                 unique_values: List[object],
                 data_types: List[type],
                 mixed_types: bool,
                 avro_path: str,
                 avro_name: str,
                 data_type_config: Optional[DataTypeConfigModel] = None,
                 parent_config: Optional[ParentColumnInfo] = None
                 ):
        """
        Column Info Object.
        Holds all infos about one analyzed column which was found in the given documents.
        :param name: column Name
        :param path: JSON path to access the column
        :param has_nulls: True if null values are present
        :param density: Percentage (0,1) of how densely the column is populated. 1 means no null-values are present
        :param unique_values: list of unique values of this column in the given documents
        :param data_types: list of found python datatypes of this column in the given documents
        :param mixed_types: True if daty_types holds more than one entry and therefore mixed types are present in this column.
        :param avro_path: AVRO path
        :param avro_name: AVRO name
        :param data_type_config: data type config
        :param parent_config: If the column is not a root element, a parent ColumnInfo object is present,
                which describes the parent column.

        """
        self.name = name
        self.path = path
        self.has_nulls = has_nulls
        self.density = density
        self.unique_values = unique_values
        self.data_types = data_types
        self.mixed_types = mixed_types
        self.data_type_config = data_type_config
        self.parent_config = parent_config
        self.avro_path = avro_path
        self.avro_name = avro_name

    def as_dict(self) -> dict:
        """
        Returns the object as a dict
        :return:
        """
        return asdict(self)


@dataclass(config=ModelConfig)
class ColumnInfos:
    column_infos: List[ColumnInfo]

    def __init__(self, column_infos: [ColumnInfo]):
        """
        ColumnInfos Object. Holds infos about all found columns which have been found in the given documents.
        :param column_infos: list of ColumnInfo objects
        """
        self.column_infos = column_infos

    def as_dict(self) -> dict:
        """
        Returns the object as a dict
        :return:
        """
        return asdict(self)
