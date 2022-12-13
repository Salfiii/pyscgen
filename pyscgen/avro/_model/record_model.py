from __future__ import annotations

import warnings
from abc import ABC, abstractmethod
from typing import List, Optional, Union, Any
from dataclasses import asdict, dataclass
from pydantic import validator
from pyscgen.avro._model.__model_config import ModelConfig


@dataclass
class AvroEntity(ABC):

    def as_dict(self, remove_empty: bool = True):
        """
        Converts the dataclass to a dict
        :param remove_empty: If True, removes all keys with None values from the avro schema.
            Defaults to True.
        :return:
        """

        avro_entity = asdict(self)

        def remove_empty_dicts(dict_: dict):
            """

            :param dict_:
            :return:
            """
            new_dict = {}
            for k, v in dict_.items():
                if isinstance(v, dict):
                    v = remove_empty_dicts(v)
                if v is not None:
                    new_dict[k] = v
            return new_dict or None

        def remove_nones(obj):
            """
            Source:
            https://stackoverflow.com/questions/20558699/python-how-recursively-remove-none-values-from-a-nested-data-structure-lists-a
            :param obj:
            :return:
            """
            if isinstance(obj, (list, tuple, set)):
                return type(obj)(remove_nones(x) for x in obj if x is not None)
            elif isinstance(obj, dict):
                return type(obj)((remove_nones(k), remove_nones(v))
                                 for k, v in obj.items() if k is not None and v is not None)
            else:
                return obj

        if remove_empty:
            avro_entity = remove_empty_dicts(remove_nones(avro_entity))

        return avro_entity

    @abstractmethod
    def add_item(self, item: Any):
        """

        :param item:
        :return:
        """
        pass

    @abstractmethod
    def find_child(self, name: str):
        """

        :param name:
        :return:
        """
        pass

    def find_child_list(self, list_: list, name: str):
        """
        :param list_:
        :param name:
        :return:
        """
        for element in list_:
            if type(element) in (Array, ArrayItem, Field, Record):
                result = element.find_child(name)
            else:
                result = None
        return result


@dataclass
class Field(AvroEntity):
    """
    AVRO-Field
    """
    name: str
    type: Union[str, List[str], Array, Record]
    logicalType: Optional[str] = None
    precision: Optional[int] = None
    scale: Optional[int] = None
    order: Optional[str] = None
    aliases: Optional[List[str]] = None
    size: Optional[int] = None
    default: Optional[Any] = None

    def __init__(self, name: str,
                 type: Union[str, List[str], Array, Record],
                 logicalType: Optional[str] = None,
                 precision: Optional[int] = None,
                 scale: Optional[int] = None,
                 order: Optional[str] = None,
                 aliases: Optional[List[str]] = None,
                 size: Optional[int] = None,
                 default: Optional[Any] = None):
        """

        :param name:
        :param type:
        :param logicalType:
        :param precision:
        :param scale:
        :param order:
        :param aliases:
        :param size:
        """
        self.name = name
        self.type = type
        self.logicalType = logicalType
        self.precision = precision
        self.scale = scale
        self.order = order
        self.aliases = aliases
        self.size = size
        self.default = default

    @validator('order')
    def name_must_contain_space(cls, v):
        """

        :param v:
        :return:
        """
        allowed: [str] = ["ascending", "descending", "ignore"]
        if v:
            for order in v:
                if order not in allowed:
                    raise ValueError("Allowed values in 'order' are: " + str(allowed))
        return v

    def find_child(self, name: str):
        """

        :param name:
        :return:
        """
        if type(self.type) in (Array, Record):
            result: Field = self.type.find_child(name)
            return result
        elif type(self.type) == list:
            result = self.find_child_list(self.type, name)
            return result

    def add_item(self, item: Any):
        """

        :param item:
        :return:
        """
        if type(self.type) == list:
            for type_ in self.type:
                if type(type_) in (Array, Record):
                    type_.add_item(item)
        elif type(self.type) in (Array, Record):
            self.type.add_item(item)


@dataclass
class ArrayItem(AvroEntity):
    """
    One Item in an AVRO-Array
    """
    name: str
    type: str
    default: Optional[Any] = None
    fields: Optional[List[Field]] = None

    def __init__(self, name: str,
                 type: str,
                 default: Optional[Any] = None,
                 fields: Optional[List[Field]] = None):
        """

        :param name:
        :param type:
        :param default:
        :param fields:
        """
        self.name = name
        self.type = type
        self.default = default
        self.fields = fields

    def add_item(self, item: Field):
        """

        :param item:
        :return:
        """
        if self.fields is None:
            self.fields = [item]
        else:
            self.fields.append(item)

    def find_child(self, name: str) -> Field:
        """

        :param name:
        :return:
        """
        result = None
        if self.name == name:
            result = self
        elif self.fields:
            for field in self.fields:
                if type(field) == list:
                    result = self.find_child_list(field, name)
                else:
                    if field.name == name:
                        result = field
        else:
            result = self
        return result


@dataclass
class Array(AvroEntity):
    """
    AVRO-Array
    """
    type: str
    default: Optional[Any] = None
    items: Optional[Union[ArrayItem, Array]] = None

    def __init__(self, type: str,
                 default: Optional[Any] = None,
                 items: Optional[Union[ArrayItem, Array]] = None):
        """

        :param type:
        :param default:
        :param items:
        """
        self.type = type
        self.default = default
        self.items = items

    def add_item(self, item: ArrayItem):
        """

        :param item:
        :return:
        """
        self.items = item

    def find_child(self, name: str) -> ArrayItem:
        """

        :param name:
        :return:
        """
        if type(self.items) == list:
            result = self.find_child_list(self.items, name)
        else:
            if self.items:
                return self.items
            else:
                result = self
        return result


@dataclass
class Record(AvroEntity):
    """
    AVRO Record
    """
    name: str
    type: Union[str, List[str], Array, Record]
    default: Optional[Any] = None
    fields: Optional[List[Field]] = None

    def __init__(self, name: str,
                 type: Union[str, List[str], Array, Record],
                 default: Optional[Any] = None,
                 fields: Optional[List[Field]] = None):
        """

        :param name:
        :param type:
        :param fields:
        """
        self.name = name
        self.type = type
        self.default = default
        self.fields = fields

    def add_item(self, item: Field):
        """

        :param item:
        :return:
        """
        if self.fields is None:
            self.fields = [item]
        else:
            self.fields.append(item)

    def find_child(self, name: str) -> Field:
        """

        :param name:
        :return:
        """
        result = None
        if self.name == name:
            result = self
        elif self.fields:
            for field in self.fields:
                if type(field) == list:
                    result = self.find_child_list(field, name)
                else:
                    try:
                        if field.name == name:
                            result = field
                    except Exception as e:
                        print(e)
        else:
            result = self
        return result


@dataclass
class Schema(AvroEntity):
    """
    AVRO-Record
    """
    name: str
    type: str = "record"
    namespace: Optional[str] = None
    fields: Optional[List[Field]] = None

    def __init__(self, name: str,
                 type: str = "record",
                 namespace: Optional[str] = None,
                 fields: List[Field] = None):
        """

        :param name:
        :param namespace:
        :param fields:
        """
        self.name = name
        self.type = type
        self.namespace = namespace
        self.fields = fields

    def add_item(self, field: Field):
        """

        :param field:
        :return:
        """
        if self.fields is None:
            self.fields = [field]
        else:
            self.fields.append(field)

    def find_child(self, name: str) -> Field:
        """

        :param name:
        :return:
        """
        result = None
        for field in self.fields:
            if type(field) == list:
                result = self.find_child_list(field, name)
            else:
                if field.name == name:
                    result = field
        return result
