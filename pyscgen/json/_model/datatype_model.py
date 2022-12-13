from typing import List, Optional
from dataclasses import asdict
from pydantic.dataclasses import dataclass

from pyscgen.json._model.__model_config import ModelConfig


@dataclass(config=ModelConfig)
class DataTypeConfigModel:
    python_type: object
    python_value: object
    avro_type: str
    avro_logical_type: Optional[str]
    avro_additional: Optional[dict]
    json_type: Optional[str]

    def __init__(self, python_type: object,
                 python_value: object,
                 avro_type: str,
                 avro_logical_type: str,
                 avro_additional: dict = None,
                 json_type: str = None):
        """

        :param python_type: Python datatype
        :param python_value: Python value
        :param avro_type: To the python datatype matching AVRO datatype.
        :param avro_logical_type: If needed, an AVRO logical type
        :param avro_additional: AVRO additional Parameters as a dictionary like "precision" or "scale"
        :param json_type: To the python datatype matching JSON datatype.
        """
        self.python_type: object = python_type
        self.python_value: object = python_value
        self.avro_type: str = avro_type
        self.avro_logical_type: str = avro_logical_type
        self.avro_additional: dict = {} if avro_additional is None else avro_additional
        self.json_type: str = json_type

    def as_dict(self) -> dict:
        """
        Returns the Object as a dict
        :return:
        """
        return asdict(self)
