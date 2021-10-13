"""
Import Libraries
"""
import pandas as pd
import tempfile
import logging
import pyodbc
import requests
import json 
import struct
import string 
import sys
import time
from itertools import islice
from adal import AuthenticationContext
from azure.core.exceptions import HttpResponseError, ResourceNotFoundError
from azure.core.exceptions import ResourceExistsError
from azure.keyvault.secrets import SecretClient
from azure.identity import DefaultAzureCredential
from azure.mgmt.search import SearchManagementClient 
from azure.mgmt.search.models import SearchService, Sku 
from azure.mgmt.storage import StorageManagementClient 
from azure.mgmt.storage.models import StorageAccountCreateParameters, Sku 
from azure.storage.blob import BlobClient 
from azure.storage.filedatalake import DataLakeServiceClient 

"""
Class
"""
class NoneError(Exception):
    pass

class AzureCredentials:
    CONST_KEY_VAULT_URL = "https://text-mining-dev-kv.vault.azure.net"

    def __get_key_list(self):
        cred_dict = {}

        credential = DefaultAzureCredential()
        secret_client = SecretClient(vault_url=self.CONST_KEY_VAULT_URL, credential=credential)
        secret_properties = secret_client.list_properties_of_secrets()

        for secret_property in secret_properties:
                cred_dict[secret_property.name] = secret_client.get_secret(secret_property.name).value
        
        return cred_dict
   
    def get_arm_credentials(self):
        credentials = self.__get_key_list()
        self.az_subscription_nonprod_id = credentials["az-subscription-nonprod"]
        self.az_resourcegroup_dev = credentials["az-resourcegroup-dev"]
    
    def get_adls_credentials(self):
        credentials = self.__get_key_list()
        self.adls_name = credentials["dls-storageAccount"]
        self.adls_conn_str = credentials["dls-connectionString"]
        self.adls_key = credentials["dls-storageAccountKey"]
        self.file_system = "enrichment"
        
    def get_sql_credentials(self):
        credentials = self.__get_key_list()
        self.server_name = credentials["inv-sqlServerName"]
        self.database_name = credentials["inv-sqlDatabaseName"]
        self.client_id = credentials["inv-sqlSPClientId"]
        self.client_secret = credentials["inv-sqlSPClientSecret"]
        self.az_tenant_id = credentials["az-tenantId"]

class AzureResourceManager(AzureCredentials):
    def __init__(self):
        super(AzureResourceManager, self).get_arm_credentials()
        self.connect_arm(self.az_subscription_nonprod_id)

    def connect_arm(self, subscription_id):
        credential = DefaultAzureCredential()

        self.search_client = SearchManagementClient(credential, subscription_id)
        self.storage_client = StorageManagementClient(credential, subscription_id)

class AzureStorageAccount():
    CONST_MAX_CHAR_LEN = 23
    CONST_NAME_HEAD = "stks"
    CONST_NAME_TAIL = "devwus2"

    def get_storage_account_name(self, document_type):
        # [determine the substring to indicate the document type in the Storage Account name]
        max_substring_len = self.CONST_MAX_CHAR_LEN - (len(self.CONST_NAME_HEAD) + len(self.CONST_NAME_TAIL))
        stks_document_type = document_type.replace("-", "")[0 : max_substring_len]

        return f"stks{stks_document_type}devwus2"

class AzureDataLakeStorage(AzureCredentials):
    def __init__(self, list_doc_type):
        self.list_doc_type = list_doc_type
        super(AzureDataLakeStorage, self).get_adls_credentials()
        self.connect_adls(self.adls_name, self.adls_conn_str)
        
    # [for assurance key vault has assigned values]
    def __validate(self, value):
        if value is None or value == "":
            raise NoneError("Azure search service name/search service key/indexer secret value is empty")
        else:
            return True
                 
    def connect_adls(self, adls_name, adls_key):
        # [validate secret values]
        if self.__validate(adls_name) and self.__validate(adls_key):
            try:
                # [connect to ADLS]
                self.service_client = DataLakeServiceClient.from_connection_string(self.adls_conn_str)
                
                self.file_system_client = self.service_client.get_file_system_client(self.file_system)
            except:
                raise

    def put_resource(self, doc_type, resource_json, resource_type):
        try:
            # [determine what the resource type is and get its ADLS blob name]
            file_name = f"{resource_type}.txt"

            # [determine what the directory name of the document type is]
            doc_type_dir = self.__get_document_type_directory_name(doc_type)

            # [use file name to get the BlobClient in ADLS]
            blob_client = BlobClient.from_connection_string(
                self.adls_conn_str,
                container_name=self.file_system + "/" + doc_type_dir,
                blob_name=file_name
            )

            # [upload the JSON as a text file to ADLS]
            blob_client.upload_blob(data=json.dumps(resource_json), overwrite=True)
        except:
            raise

    def __get_document_type_directory_name(self, doc_type):
        doc_type_dir = doc_type.replace("-", " ").replace("and", "&")
        doc_type_dir = string.capwords(doc_type_dir)

        return doc_type_dir
            
    def get_resource(self, doc_type, resource_type):
        try:
            # [determine what the resource type is and get its ADLS blob name]
            file_name = f"{resource_type}.txt"
            
            # [determine what the directory name of the document type is]
            doc_type_dir = self.__get_document_type_directory_name(doc_type)

            # [use file name to get the BlobClient in ADLS]
            blob_client = BlobClient.from_connection_string(
                self.adls_conn_str,
                container_name=self.file_system + "/" + doc_type_dir,
                blob_name=file_name
            )
            
            # [download the blob using the BlobClient]
            stream = blob_client.download_blob()
            data = stream.readall()
            
            return data.decode("utf-8") 
        except:
            raise

    def file_system_exists(self):
        return self.service_client.get_file_system_client(self.file_system).exists()
            
    def all_resources_exist(self, doc_type, resource_types):
        all_exist = True
        
        for resource_type in resource_types:
            try:
                # [determine what the resource type is and get its ADLS blob name]
                file_name = f"{resource_type}.txt"
                
                # [determine what the directory name of the document type is]
                doc_type_dir = self.__get_document_type_directory_name(doc_type)

                # [use file name to get the BlobClient in ADLS]
                blob_client = BlobClient.from_connection_string(
                    self.adls_conn_str,
                    container_name=self.file_system + "/" + doc_type_dir,
                    blob_name=file_name
                )

                if not blob_client.exists():
                    all_exist = False
                    break
            except:
                raise
        
        return all_exist
    
    def create_default_copies(self, doc_type, resource_types):
        # [determine what the directory name of the document type is]
        doc_type_dir = self.__get_document_type_directory_name(doc_type)

        try:
            # [create a new directory for the new document type]
            self.file_system_client.create_directory(doc_type_dir)

            for resource_type in resource_types:
                # [get the default resource JSON]
                default_resource = self.get_resource("default", resource_type)
                default_json = json.loads(default_resource)
                default_json.pop("@odata.context", None)
                default_json.pop("@odata.etag", None)

                # [change the name and other relevant fields of the resource to the document type]
                default_json["name"] = doc_type

                if resource_type == "skillset":
                    for table_json in default_json["knowledgeStore"]["projections"][0]["tables"]:
                        if table_json["generatedKeyName"] == "Documentid":
                            table_json["tableName"] = doc_type_dir.split()[0] + "Document"
                        if table_json["generatedKeyName"] == "Imagesid":
                            table_json["tableName"] = doc_type_dir.split()[0] + "Images"
                    default_json["knowledgeStore"]["projections"][0]["objects"][0]["storageContainer"] = doc_type.split("-")[0] + "projections"
                    default_json["knowledgeStore"]["projections"][0]["objects"][0]["generatedKeyName"] = doc_type.split("-")[0] + "projectionsKey"

                if resource_type == "datasource":
                    default_json["container"]["query"] = doc_type_dir
                    default_json["credentials"]["connectionString"] = self.adls_conn_str

                if resource_type == "indexer":
                    default_json["dataSourceName"] = doc_type
                    default_json["skillsetName"] = doc_type
                    default_json["targetIndexName"] = doc_type

                # [put the updated resource JSON in the new document type directory]
                self.put_resource(doc_type, default_json, resource_type)
        except:
            raise

class AzureCognitiveSearch(AzureCredentials):
    def __init__(self, list_doc_type):
        self.list_doc_type = list_doc_type
        super(AzureCognitiveSearch, self).get_adls_credentials()
        self.arm = AzureResourceManager()

    # [for assurance key vault has assigned values]
    def __validate(self, value):
        if value is None or value == "":
            raise NoneError("Azure search service name/search service key/indexer secret value is empty")
        else:
            return True

    def __get_search_resource(self, search_service, key, resource, resource_type):
        api_version = "2020-06-30-Preview"
        headers = {
            "Content-Type": "application/json",
            "api-key": key
        }

        # [validate secret values]
        if self.__validate(search_service) and self.__validate(key):
            try:
                # [determine what the resource type is and get its Azure Search REST API subdomain string]
                if resource_type == "datasource":
                    uri_resource_type = "datasources"
                if resource_type == "skillset":
                    uri_resource_type = "skillsets"
                if resource_type == "index":
                    uri_resource_type = "indexes"
                if resource_type == "indexer":
                    uri_resource_type = "indexers"
                    
                # [get the JSON definition of the Search resource]
                get_uri = f"https://{search_service}.search.windows.net/{uri_resource_type}/{resource}?api-version={api_version}"
                resource_json = requests.get(get_uri, headers=headers).json()
                return json.dumps(resource_json)
            except:
                raise
                
    def __put_search_resource(self, search_service, key, resource_name, resource_json, resource_type):
        api_version = "2020-06-30-Preview"
        headers = {
            "Content-Type": "application/json",
            "api-key": key
        }

        # [validate secret values]
        if self.__validate(search_service) and self.__validate(key):
            try:
                # [determine what the resource type is and get its Azure Search REST API subdomain string]
                if resource_type == "datasource":
                    uri_resource_type = "datasources"
                if resource_type == "skillset":
                    uri_resource_type = "skillsets"
                if resource_type == "index":
                    uri_resource_type = "indexes"
                if resource_type == "indexer":
                    uri_resource_type = "indexers"
                    
                # [prepare the JSON definition of the ADLS resource]
                adls_resource = json.loads(resource_json)
                adls_resource.pop("@odata.context", None)
                adls_resource.pop("@odata.etag", None)
                
                # [put the JSON definition of the ADLS resource]
                put_uri = f"https://{search_service}.search.windows.net/{uri_resource_type}/{resource_name}?api-version={api_version}"
                response = requests.put(put_uri, headers=headers, data=json.dumps(adls_resource))
                print(response)
            except:
                raise

    def __get_azure_search_service_name(self, document_type):
        return f"srch-{document_type}-dev-wus2"

    def __get_azure_search_service_key(self, service_name):
        return self.arm.search_client.admin_keys.get(self.arm.az_resourcegroup_dev, service_name).primary_key

    def get_azure_search_service_name(self, document_type):
        return self.__get_azure_search_service_name(document_type)

    def get_resource(self, doc_type, resource_type):
        if not self.list_doc_type is None:
            service_name = self.__get_azure_search_service_name(doc_type)
            service_key = self.__get_azure_search_service_key(service_name)
            
            return self.__get_search_resource(service_name, service_key, doc_type, resource_type)
            
    def put_resource(self, doc_type, resource_json, resource_type):
        if not self.list_doc_type is None:
            service_name = self.__get_azure_search_service_name(doc_type)
            service_key = self.__get_azure_search_service_key(service_name)
            
            return self.__put_search_resource(service_name, service_key, doc_type, resource_json, resource_type)

class SqlServer(AzureCredentials):
    CONST_DRIVER = "{ODBC Driver 17 for SQL Server}"
    
    def __init__(self):
        super(SqlServer, self).get_sql_credentials()

    def __generate_connection_string(self):
        conn_string = "DRIVER={};SERVER={};PORT=1433;DATABASE={};".format(self.CONST_DRIVER, self.server_name, self.database_name)
        return conn_string

    def __generate_token_struct(self):
        auth_context = AuthenticationContext(f"https://login.microsoftonline.com/{self.az_tenant_id}")
        token_response = auth_context.acquire_token_with_client_credentials("https://database.windows.net", self.client_id, self.client_secret)
        token_bytes = bytes(token_response["accessToken"], "UTF-8")
        exp_token = b""

        for i in token_bytes:
            exp_token += bytes({ i })
            exp_token += bytes(1)
        token_struct = struct.pack("=i", len(exp_token)) + exp_token
        return token_struct

    def __initialize_sql_account(self, token_struct):
        SQL_COPT_SS_ACCESS_TOKEN = 1256
        try:
            conn_string = self.__generate_connection_string()
            db_connect = pyodbc.connect(conn_string, attrs_before={SQL_COPT_SS_ACCESS_TOKEN: token_struct})
        except Exception as e:
            print(e)
        finally:
            return db_connect

    def execute_query(self, sql_query, sql_params, is_commit = 0):
        results = None
        connection = None
        while True:
            # [connection setup]
            if not connection:
                token_struct = self.__generate_token_struct()
                connection = self.__initialize_sql_account(token_struct)
                cursor = connection.cursor()

            # [execute sql query]
            try:
                cursor.execute(sql_query, sql_params)
                if is_commit:
                    connection.commit()
                else:
                    results = pd.DataFrame([tuple(t) for t in cursor.fetchall()])
            except pyodbc.Error as pe:
                # [for connection timeout]
                print("Error", pe)
                if pe.args == "08S01":
                    try:
                        connection.close()
                    except:
                        pass
                    connection = None
                    continue
                raise
            finally:
                break
        return results
    
    def get_document_type_name_format(self, doc_type):
        doc_type_dir = doc_type.replace("-", " ").replace("and", "&")
        doc_type_dir = string.capwords(doc_type_dir)

        return doc_type_dir

"""
Main
"""
def connect_and_execute_sql_query(sql):
    # [log]
    print("Executing SQL query...")
    time.sleep(5)

    # [execute sql query]
    sql_query = "SELECT LOWER(REPLACE(REPLACE(KMDevDocumentType3, '&', 'AND'),' ','-')) " \
                "FROM tbl_document WITH (NOLOCK) " \
                "WHERE 1 = 1 " \
                "AND DocumentStatus = ? " \
                "AND ISNULL(KMDevDocumentType3, '') <> '' " \
                "AND ISNULL(InvestmentId, '') <> '' " \
                "GROUP BY KMDevDocumentType3"
    sql_params = ("New")
    sql_results = sql.execute_query(sql_query, sql_params)
    sql_results.rename(columns = { 0: "DocumentType" }, inplace=True)
    return sql_results

def deploy_cogsearch_services(list_document_type, search, arm):
    try:
        print("Checking if each document type's Search Service exists in Azure...")
        for doc_type in list_document_type:
            # [get the Search Service name of each document type]
            search_service_name = search.get_azure_search_service_name(doc_type)

            # [check if each document type's Search Service exists in Azure]
            resource_found = False
            for search_service in arm.search_client.services.list_by_resource_group(arm.az_resourcegroup_dev):
                if search_service.name == search_service_name:
                    resource_found = True
                    break 
            
            # [deploy a Search Service if one doesn't already exist]
            if not resource_found:
                print("   ", search_service_name, "does NOT exist. Deploying new Search Service...")

                # [initialize a new Search Service object]
                service = SearchService(location="West US 2")
                service_sku = Sku(name="standard")
                service.sku = service_sku

                # [deploy the new Search Service to Azure]
                arm.search_client.services.begin_create_or_update(
                    arm.az_resourcegroup_dev,
                    search_service_name,
                    service
                )
                print("   ", search_service_name, "has been successfully deployed!")
            else:
                print("   ", search_service_name, "does exist. No deployment needed.")
        print()
    except Exception as e:
        print(e)

def deploy_knowledge_store_resources(document_type, adls, st, arm):
    # [get the ADLS skillset JSON]
    adls_skillset = adls.get_resource(document_type, "skillset")
    adls_skillset = json.loads(adls_skillset)
    adls_skillset.pop("@odata.context", None)
    adls_skillset.pop("@odata.etag", None)

    # [get the ADLS indexer JSON]
    adls_indexer = adls.get_resource(document_type, "indexer")
    adls_indexer = json.loads(adls_indexer)
    adls_indexer.pop("@odata.context", None)
    adls_indexer.pop("@odata.etag", None)

    # [check if the ADLS skillset has a knowledge store storage connection string defined]
    print("   ", "Checking", document_type + "'s ADLS skillset for a knowledge store storage connection string definition...")
    if "knowledgeStore" in adls_skillset:
        if adls_skillset["knowledgeStore"] != None:
            if "storageConnectionString" in adls_skillset["knowledgeStore"]:
                if adls_skillset["knowledgeStore"]["storageConnectionString"] != None:
                    print("   ", document_type + "'s ADLS skillset has a 'storageConnectionString' defined. No deployment needed.")
                # [if the ADLS skillset doesn't have a knowledge store storage connection string defined, deploy a new Storage Account.]
                else:
                    print("   ", document_type + "'s ADLS skillset does NOT have a 'storageConnectionString' defined. Deploying a new Storage Account...")

                    # [get the name of the new Storage Account]
                    account_name = st.get_storage_account_name(document_type)

                    # [initialize a new Storage Account parameters object]
                    storage_sku = Sku(
                        name="Standard_RAGRS",
                        tier="Standard"
                    )
                    parameters = StorageAccountCreateParameters(
                        sku=storage_sku,
                        kind="StorageV2",
                        location="West US 2",
                        access_tier="Hot",
                        enable_https_traffic_only=True,
                        allow_blob_public_access=False,
                        minimum_tls_version="TLS1_2",
                    )

                    # [deploy the new Storage Account to Azure]
                    arm.storage_client.storage_accounts.begin_create(
                        arm.az_resourcegroup_dev,
                        account_name,
                        parameters
                    )

                    # [wait until the Storage Account has been successfully provisioned]
                    provisioning_state = arm.storage_client.storage_accounts.get_properties(
                            arm.az_resourcegroup_dev,
                            account_name
                        ).provisioning_state

                    while(provisioning_state != "Succeeded"):
                        provisioning_state = arm.storage_client.storage_accounts.get_properties(
                            arm.az_resourcegroup_dev,
                            account_name
                        ).provisioning_state

                    # [get the new Storage Account's connection string]
                    endpoints_protocol = "https"
                    account_key = arm.storage_client.storage_accounts.list_keys(
                        arm.az_resourcegroup_dev,
                        account_name,
                    ).keys[0].value
                    conn_str = f"DefaultEndpointsProtocol={endpoints_protocol};AccountName={account_name};AccountKey={account_key};EndpointSuffix=core.windows.net"
                    print("   ", document_type + "'s knowledge store Storage Account has been successfully deployed!")

                    # [update the ADLS skillset with the new knowledge store storage connection string]
                    adls_skillset["knowledgeStore"]["storageConnectionString"] = conn_str

                    # [update the ADLS skillset file with the version with the new knowledge store storage connection string]
                    print("   ", "Updating", document_type + "'s ADLS skillset...")
                    adls.put_resource(document_type, adls_skillset, "skillset")
                    print("   ", document_type + "'s ADLS skillset has been successfully updated!")

                    # [update the ADLS indexer with the new knowledge store storage connection string]
                    adls_indexer["cache"]["storageConnectionString"] = conn_str

                    # [update the ADLS indexer file with the version with the new knowledge store storage connection string]
                    print("   ", "Updating", document_type + "'s ADLS indexer...")
                    adls.put_resource(document_type, adls_indexer, "indexer")
                    print("   ", document_type + "'s ADLS indexer has been successfully updated!")
            else:
                print("   ", document_type + "'s skillset has no 'storageConnectionString' field. Please add a 'storageConnectionString' field to its .txt file in ADLS.")
        else:
            print("   ", document_type + "'s skillset has no 'knowledgeStore' field. Please add a 'knowledgeStore' field to its .txt file in ADLS.")

def update_cogsearch_resources(list_document_type, search, adls, st, arm):
    resource_types = ["skillset", "index", "datasource", "indexer"]
    
    # [check if "enrichment" file system exists in ADLS]
    print("Checking that the 'enrichment' file system exists...")
    if(adls.file_system_exists()):
        print("   ", "The 'enrichment' file system exists!")
        print()
        for doc_type in list_document_type:
            print("Checking", doc_type, "for all Search resource types...")
            # [check if ADLS has all the resources defined for that document type]
            if not adls.all_resources_exist(doc_type, resource_types):
                print("   ", doc_type, "does NOT have all resource types.")
                print("   ", "Deploying default resources for", doc_type + "...")
                adls.create_default_copies(doc_type, resource_types)
            else:
                print("   ", doc_type, "has all Search resource types!")
                
            for resource_type in resource_types:
                # [ensure an azure search resource exists for each document type]
                if resource_type == "skillset":
                    deploy_knowledge_store_resources(doc_type, adls, st, arm)

                # [get the Search resource JSON]
                search_resource = search.get_resource(doc_type, resource_type)
                search_json = json.loads(search_resource)
                search_json.pop("@odata.context", None)
                search_json.pop("@odata.etag", None)
                
                # [get the ADLS resource JSON]
                adls_resource = adls.get_resource(doc_type, resource_type)
                adls_json = json.loads(adls_resource)
                adls_json.pop("@odata.context", None)
                adls_json.pop("@odata.etag", None)

                # [if the ADLS JSON doesn't match the Azure Search JSON, update it with the ADLS JSON]
                if search_json != adls_json:
                    print("   ", doc_type + "'s Search", resource_type, "does NOT match its ADLS", resource_type + ".")
                    print("   ", "Updating the", doc_type, resource_type + "...")
                    search.put_resource(doc_type, adls_resource, resource_type)
                else:
                    print("   ", doc_type + "'s Search", resource_type, "matches its ADLS", resource_type + ".")
            print()
    else:
        print("   ", "The 'enrichment' file system does NOT exist. Create one to continue dynamic deployment of Search resources.")
        print()

def update_sql_skills(list_document_type, sql, search):
    all_skills = [] 

    # [get the skill names and skill descriptions each document type has]
    for doc_type in list_document_type:
        # [get the search skillset json]
        search_resource = search.get_resource(doc_type, "skillset")
        search_json = json.loads(search_resource)
        search_json.pop("@odata.context", None)
        search_json.pop("@odata.etag", None)

        for skill in search_json["skills"]:
            all_skills.append((skill["name"], skill["description"], doc_type))

    i = 0
    for skill in all_skills:
        name = skill[0]
        desc = skill[1]

        # [log merge skill query]
        ctr = i
        print("Updating record", ctr + 1, "of", len(all_skills), "in tbl_skill...")

        # [build merge skill sql query]
        merge_skill_sql_query = "MERGE INTO tbl_skill AS Target " \
                    "USING (SELECT * FROM " \
                    "(VALUES (?, ?)) " \
                    "AS s (Name, Description) " \
                    ") AS Source " \
                    "ON (Target.Name = Source.Name AND Target.Description = Source.Description) " \
                    "WHEN NOT MATCHED THEN " \
                    "INSERT (Name, Description) " \
                    "VALUES (?, ?);"
        merge_skill_sql_params = (name, desc, name, desc)

        # [execute sql query]
        sql.execute_query(merge_skill_sql_query, merge_skill_sql_params, 1)
        i += 1
    print()

    i = 0
    for skill in all_skills:
        skill_name = skill[0]
        doc_type = skill[2]

        # [determine what the sql name of the document type is]
        sql_doc_type = sql.get_document_type_name_format(doc_type)

        # [log merge type_skill query]
        ctr = i
        print("Updating record", ctr + 1, "of", len(all_skills), "in tbl_type_skill...")

        # [build merge type_skill sql query]
        merge_type_skill_sql_query = "MERGE INTO tbl_type_skill AS Target " \
                    "USING (SELECT * FROM " \
                    "(VALUES ((SELECT TypeId " \
                    "FROM tbl_type " \
                    "WHERE Type = ?), " \
                    "(SELECT SkillId " \
                    "FROM tbl_skill " \
                    "WHERE Name = ?))) " \
                    "AS s (TypeId, SkillId) " \
                    ") AS Source " \
                    "ON (Target.TypeId = Source.TypeId AND Target.SkillId = Source.SkillId) " \
                    "WHEN NOT MATCHED THEN " \
                    "INSERT (TypeId, SkillId) " \
                    "VALUES ((SELECT TypeId " \
                    "FROM tbl_type " \
                    "WHERE Type = ?), " \
                    "(SELECT SkillId " \
                    "FROM tbl_skill " \
                    "WHERE Name = ?));" 
        merge_type_skill_sql_params = (sql_doc_type, skill_name, sql_doc_type, skill_name)

        # [execute merge type_skill sql query]
        sql.execute_query(merge_type_skill_sql_query, merge_type_skill_sql_params, 1)
        i += 1
    print()

def execute():
    # [connect to azure search, adls, and azure resource manager]
    sql = SqlServer()

    # [run sql query to determine which document type needs to be indexed]
    sql_results = connect_and_execute_sql_query(sql)
    if len(sql_results) == 0:
        print("No SQL matching records found")
        print("Enrichment: automatic indexing complete!")
        return False

    document_type = []
    for result in sql_results.itertuples(index=False):
        document_type += result

    print("Document type indexers to execute:")
    for doc_type in document_type:
        print("   ", doc_type)
    print()

    # [connect to azure search, adls, and azure resource manager]
    search = AzureCognitiveSearch(document_type)
    adls = AzureDataLakeStorage(document_type)
    arm = AzureResourceManager()
    st = AzureStorageAccount()
        
    # [ensure an azure search resource exists for each document type]
    deploy_cogsearch_services(document_type, search, arm)

    # [ensure all cognitive search resources json definitions match adls json definitions]
    update_cogsearch_resources(document_type, search, adls, st, arm)

    # [ensure sql skill tables match azure search skillset skills]
    update_sql_skills(document_type, sql, search)

    return True

def main(name: str) -> str:
    execute()
    return "Done dynamically deploying all resources."

main("Run")