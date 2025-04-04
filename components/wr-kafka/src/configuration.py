import logging
from pydantic import BaseModel, Field, ValidationError, field_validator
from keboola.component.exceptions import UserException
from enum import Enum
import json


class Units(str, Enum):
    metric = "metric"
    imperial = "imperial"


class Configuration(BaseModel):
    servers: list[str] = Field(default=None)
    group_id: str = Field(default=None)
    client_id: str = Field(default=None)

    topic: str

    security_protocol: str = Field(default=None)
    sasl_mechanisms: str = Field(default=None)

    username: str = Field(default=None)
    password: str = Field(alias="#password", default=None)

    ssl_ca: str = Field(default=None)
    ssl_key: str = Field(default=None)
    ssl_certificate: str = Field(default=None)

    begin_offsets: str = Field(default=None)

    kafka_extra_params: str = Field(default=None)

    key_column_name: str = ""
    value_column_names: list[str] = []
    serialize: str = Field(default=None)
    schema_str: str = Field(default=None)
    schema_registry_url: str = Field(default=None)
    schema_registry_extra_params: str = Field(default={})

    debug: bool = False
    freeze_timestamp: bool = False

    def __init__(self, **data):
        try:
            super().__init__(**data)
        except ValidationError as e:
            error_messages = [f"{err['loc'][0]}: {err['msg']}" for err in e.errors()]
            raise UserException(f"Validation Error: {', '.join(error_messages)}")

        if self.debug:
            logging.debug("Component will run in Debug mode")

    @field_validator("kafka_extra_params", "schema_registry_extra_params")
    def parse_configuration(cls, value):
        if isinstance(value, str):
            try:
                return json.loads(value.replace("'", '"'))
            except json.JSONDecodeError as e:
                raise ValueError(f"Invalid JSON string for config_params: {e}")
        return value
