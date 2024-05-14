from dataclasses import dataclass, field
from typing import Optional, Dict

from supersetapiclient.base import Object, ObjectFactories, default_string


@dataclass
class EngineInformation:
    disable_ssh_tunneling: bool = False  # Default values provided
    supports_file_upload: bool = False

    def to_json(self):
        return {
            "disable_ssh_tunneling": self.disable_ssh_tunneling,
            "supports_file_upload": self.supports_file_upload,
        }


@dataclass
class Database(Object):
    JSON_FIELDS = [
        "extra",  # Ensures JSON serialization handles 'extra' properly
    ]

    database_name: str
    engine: str
    driver: str
    sqlalchemy_uri_placeholder: str
    expose_in_sqllab: bool
    configuration_method: str
    engine_information: EngineInformation = field(
        default_factory=EngineInformation)
    # Changed from json_field to field
    extra: dict = field(default_factory=dict)
    parameters: Dict[str, dict] = field(default_factory=dict)

    id: Optional[int] = None
    allow_ctas: bool = True
    allow_cvas: bool = True
    allow_dml: bool = True
    allow_multi_schema_metadata_fetch: bool = True
    allow_run_async: bool = True
    cache_timeout: Optional[int] = None
    encrypted_extra: str = default_string()
    masked_encrypted_extra: str = default_string()
    force_ctas_schema: str = default_string()
    server_cert: str = default_string()
    sqlalchemy_uri: str = default_string()

    def to_json(self, *args, **kwargs):
        if not self.extra:
            self.extra = {
                "metadata_params": {},
                "engine_params": {},
                "metadata_cache_timeout": {},
                "schemas_allowed_for_csv_upload": [],
            }
        data = super().to_json(*args, **kwargs)
        data.update({
            "engine_information": self.engine_information.to_json(),
            "parameters": self.parameters,
        })
        return data

    def run(self, query, query_limit=None):
        return self._parent.client.run(database_id=self.id, query=query, query_limit=query_limit)

    def test_connection(self):
        return self._parent.test_connection(self)


class Databases(ObjectFactories):
    endpoint = "database/"
    base_object = Database

    # Hardcoded columns for adding and editing databases
    _add_columns = [
        "database_name", "engine", "driver", "sqlalchemy_uri_placeholder",
        "expose_in_sqllab", "configuration_method", "engine_information",
        "extra", "masked_encrypted_extra", "parameters", "allow_ctas",
        "allow_cvas", "allow_dml", "allow_multi_schema_metadata_fetch",
        "allow_run_async", "cache_timeout", "encrypted_extra",
        "force_ctas_schema", "server_cert", "sqlalchemy_uri"
    ]

    _edit_columns = [
        "database_name", "sqlalchemy_uri", "extra", "parameters",
        "allow_ctas", "allow_cvas", "allow_dml", "allow_multi_schema_metadata_fetch",
        "allow_run_async", "cache_timeout", "expose_in_sqllab", "configuration_method",
        "engine_information", "masked_encrypted_extra", "encrypted_extra",
        "force_ctas_schema", "server_cert"
    ]

    @property
    def add_columns(self):
        return self._add_columns

    @property
    def edit_columns(self):
        return self._edit_columns

    @property
    def test_connection_url(self):
        """Base url for testing connections to a database."""
        return self.client.join_urls(self.client.base_url, self.endpoint, "test_connection")

    def test_connection(self, obj):
        """Test connection to a database"""
        url = self.test_connection_url
        connection_columns = ["database_name", "sqlalchemy_uri"]
        o = {c: getattr(obj, c) for c in connection_columns}
        response = self.client.post(url, json=o)
        return response.json().get("message") == "OK"
