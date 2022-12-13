import pytest
from pyscgen.__config.dtype_config import DataTypeConfig


class TestDataTypeConfig:

    def test_get_dtype_config(self):
        dtype_config = DataTypeConfig()
        assert type(dtype_config.data_type_config) == dict

    def test_set_dtype_config(self):
        dtype_config = DataTypeConfig()
        config: dict = dtype_config.data_type_config
        config["AdditionalElement"] = {
                "python_type": type(None),
                "python_value": None,
                "avro_type": "null",
                "json_type": "null"
            }
        dtype_config.data_type_config = config
        assert type(dtype_config.data_type_config) == dict

    def test_set_dtype_config_exception(self):
        dtype_config = DataTypeConfig()
        config: dict = dtype_config.data_type_config.copy()
        config.pop("None", None)
        with pytest.raises(DataTypeConfig.ContainsNotAllNeededConfigEntriesException):
            dtype_config.data_type_config = config

