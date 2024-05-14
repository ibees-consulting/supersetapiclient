import sys
import os
from uuid import UUID
import json
from supersetapiclient.client import SupersetClient
from supersetapiclient.databases import Database, Databases, EngineInformation

# Modify the Python path to include the directory two levels up
files_path = os.path.join(os.path.dirname(__file__), '../../')
sys.path.insert(0, files_path)
print(files_path, "files_path********")

# Setting this environment variable for OAuth settings, assuming insecure transport is needed for localhost testing
os.environ['OAUTHLIB_INSECURE_TRANSPORT'] = '1'

# Instantiate the Superset API client
client = SupersetClient(
    host="http://localhost:8088",
    username="admin",
    password="admin"
)

# Create an instance of EngineInformation
engine_information = EngineInformation(
    disable_ssh_tunneling=True,
    supports_file_upload=True
)

service_account_credentials = json.dumps(
    {
        "credentials_info": {
            "type": "service_account",
            "project_id": "",
            "private_key_id": "",
            "private_key": "",
            "client_email": "",
            "client_id": "",
            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
            "token_uri": "https://oauth2.googleapis.com/token",
            "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
            "client_x509_cert_url": "",
            "universe_domain": "googleapis.com"
        },
    }
)
# Serialize the service account credentials to a JSON string
credentials_info_json = service_account_credentials
# Create a Database instance with comprehensive parameters including nested structures
database_instance = Database(
    database_name="Google BigQuery",
    engine="bigquery",
    driver="bigquery",
    sqlalchemy_uri_placeholder="bigquery://{project_id}",
    expose_in_sqllab=True,
    configuration_method="dynamic_form",
    engine_information=engine_information,
    # Passing extra as a dictionary
    extra=json.dumps({"allows_virtual_table_explore": True}),
    # Very Important to keep both encrypted_extra and masked_encrypted_extra
    masked_encrypted_extra=service_account_credentials,
    encrypted_extra=service_account_credentials
    # parameters=credentials_info_json
)

# Attempt to add the new database to Superset
try:
    databases_api = Databases(client)
    added_database = databases_api.add(database_instance)
    print("Database added successfully:", added_database)
except Exception as e:
    print("Failed to add database:", str(e))
