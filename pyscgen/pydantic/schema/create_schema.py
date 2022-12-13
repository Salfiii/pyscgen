from pydantic_avro.avro_to_pydantic import avsc_to_pydantic
from pyscgen.avro.schema.create_schema import AvroSchemaGenerator


class PydanticSchemaGenerator:

    def __init__(self, alphabetically_ordered_by_path: bool = True, debug: bool = False):
        """

        :param alphabetically_ordered_by_path:
            If true, outputs the fields in the AVRO-Schema alphabetically ordered by name.
            This comes in handy if you need a more deterministic solution.
        :param debug: If True, Debug inf is printed out.
            Defaults to False.
        """
        self.avro_schema_generator = AvroSchemaGenerator(alphabetically_ordered_by_path=alphabetically_ordered_by_path, debug=debug)
        self.debug = debug

    def create_schema(self, docs: [dict], name: str = "PyScGenClass", namespace: str = "com.pyscgen.avro") -> str:
        """
        Creates a pydantic schema/model based on some JSON Messages
        :param docs: List of dicts on which data the AVRO-Schema will be based on
        :param name: Name of the AVRO-Schema used in the "name" Attribute
        :param namespace: AVRO Namespace.
        :return:
        """
        avro_schema = self.avro_schema_generator.create_schema(docs=docs, name=name, namespace=namespace)
        pydantic_schema = avsc_to_pydantic(avro_schema.as_dict(remove_empty=True))
        return pydantic_schema

