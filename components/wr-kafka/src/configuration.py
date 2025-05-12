import json
import logging
from enum import Enum

from keboola.component.exceptions import UserException
from pydantic import BaseModel, Field, ValidationError, field_validator


class Units(str, Enum):
    metric = "metric"
    imperial = "imperial"


class Configuration(BaseModel):
    bootstrap_servers: list[str] = Field(default=None)
    client_id: str = Field(default=None)

    topic: str = Field(default=None)

    security_protocol: str = Field(default=None)
    sasl_mechanism: str = Field(default=None)

    username: str = Field(default=None)
    password: str = Field(alias="#password", default=None)

    ssl_ca: str = Field(alias="#ssl_ca", default=None)
    ssl_key: str = Field(alias="#ssl_key", default=None)
    ssl_certificate: str = Field(alias="#ssl_certificate", default=None)

    kafka_extra_params: str = Field(default=None)

    key_column_name: str = ""
    value_column_names: list[str] = []
    serialize: str = Field(default=None)
    schema_str: str = Field(default=None)
    schema_registry_url: str = Field(default=None)
    schema_registry_extra_params: str = Field(default={})

    debug: bool = False

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
        if value and isinstance(value, str):
            try:
                return json.loads(value.replace("'", '"'))
            except json.JSONDecodeError as e:
                raise ValueError(f"Invalid JSON string for config_params: {e}")
        return value
