from typing import List, Optional
from pydantic.dataclasses import dataclass
from dataclasses import asdict
from pyscgen.json._model.__model_config import ModelConfig


@dataclass(config=ModelConfig)
class Column:
    name: str
    path: str
    is_null: bool
    data_type: type
    value: object

    def __init__(self, name: str, path: str, is_null: bool, data_type: type, value: object):
        """

        :param name: Column Name.
        :param path: Path to the column.
        :param is_null: True if the value is null, else False.
        :param data_types: Python data type of the column.
        :param unique_values: List of unique values in the column
        """
        self.name = name
        self.path = path
        self.is_null = is_null
        self.data_type = data_type
        self.value = value

    def as_dict(self) -> dict:
        """
        Return the Object as dict
        :return:
        """
        return asdict(self)


@dataclass(config=ModelConfig)
class Document:
    columns: List[Column]
    size: int

    def __init__(self, columns: List[Column], size: int):
        """

        :param columns: List of Columns within the document
        :param size: Number of columns
        """
        self.columns = columns
        self.size = size

    def as_dict(self) -> dict:
        """
        Return the Object as dict
        :return:
        """
        return asdict(self)


@dataclass(config=ModelConfig)
class Collection:
    documents: List[Document]
    size: int

    def __init__(self, documents: List[Document], size: int):
        """

        :param documents: List of Documents in the Collection
        :param size: Number of documents
        """
        self.documents = documents
        self.size = size

    def as_dict(self) -> dict:
        """
        Return the Object as dict
        :return:
        """
        return asdict(self)
