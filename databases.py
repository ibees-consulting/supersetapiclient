from supersetapiclient.base import ObjectFactories
from dataclasses import dataclass, field
from typing import Optional, Dict

from supersetapiclient.base import Object, ObjectFactories, default_string, json_field

@dataclass
class Database(Object):
    JSON_FIELDS = ["extra", "masked_encrypted_extra", "parameters"]

    database_name: str
    sqlalchemy_uri: str
    allow_ctas: bool = True
    allow_cvas: bool = True
    allow_dml: bool = True
    allow_file_upload: bool = True
    allow_run_async: bool = True
    cache_timeout: Optional[int] = None
    configuration_method: str = "sqlalchemy_form"
    driver: Optional[str] = None
    engine: Optional[str] = None
    expose_in_sqllab: bool = True
    external_url: Optional[str] = None
    extra: Dict = field(default_factory=dict)
    force_ctas_schema: Optional[str] = None
    impersonate_user: bool = False
    is_managed_externally: bool = False
    masked_encrypted_extra: Dict = field(default_factory=dict)
    encrypted_extra: Dict = field(default_factory=dict)
    parameters: Dict = field(default_factory=dict)
    server_cert: Optional[str] = None
    ssh_tunnel: Optional[Dict] = None
    uuid: Optional[str] = None

    def to_json(self, *args, **kwargs):
        data = super().to_json(*args, **kwargs)
        # Convert dictionary fields to JSON string if not already handled
        for field_name in self.JSON_FIELDS:
            if hasattr(self, field_name):
                data[field_name] = json.dumps(getattr(self, field_name))
        return data

    @classmethod
    def from_json(cls, data: dict):
        # Ensure JSON fields are loaded correctly
        for field_name in cls.JSON_FIELDS:
            if field_name in data and isinstance(data[field_name], str):
                data[field_name] = json.loads(data[field_name])
        return super().from_json(data)


class Databases(ObjectFactories):
    endpoint = "database/"
    base_object = Database

    @property
    def test_connection_url(self):
        """Base url for these objects."""
        return self.client.join_urls(self.client.base_url, self.endpoint, "test_connection")

    def test_connection(self, obj):
        """Test connection to a database by constructing a payload that includes new fields if necessary."""
        # Build the payload using all necessary attributes that might affect the connection
        payload = {
            "database_name": obj.database_name,
            "sqlalchemy_uri": obj.sqlalchemy_uri,
            "impersonate_user": obj.impersonate_user,
            "extra": json.dumps(obj.extra),
            "server_cert": obj.server_cert,
            "encrypted_extra": json.dumps(obj.masked_encrypted_extra)  # Assuming we use this for secure fields
        }
        # Including parameters and ssh_tunnel if they are utilized
        if obj.parameters:
            payload["parameters"] = json.dumps(obj.parameters)
        if obj.ssh_tunnel:
            payload["ssh_tunnel"] = json.dumps(obj.ssh_tunnel)

        response = self.client.post(self.test_connection_url, json=payload)
        return response.json().get("message") == "OK"

    # Add more methods here as needed for CRUD operations
