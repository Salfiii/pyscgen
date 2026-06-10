from pydantic import ConfigDict


class ModelConfig:
    """
    Parent config
    """
    arbitrary_types_allowed = True


# For Pydantic v2 dataclasses
model_config = ConfigDict(arbitrary_types_allowed=True)
