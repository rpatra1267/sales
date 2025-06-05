# standard library imports
import os
import uuid
import json
import yaml
from retry import retry
import requests
from urllib.parse import quote
from collections import defaultdict
from datetime import datetime, timezone, timedelta
import copy
import ast
import jwt
import re

# metadata parser import


# third-party library imports
import pandas as pd
from jinja2 import Template

# GCP cloud library imports
from genie.core.data_extraction.data_source.sales.metadata_utils import *
from genie.core.data_extraction.data_source.sales.gcp_utils import GcpUtils
from genie.core.data_extraction.data_source.sales.common_utils import CommonUtils
from genie.util.hashicorp_vault import HashiCorpVault
from genie.core.data_extraction.table_log_executor import update_log_tables, fetch_records_to_download, log_datastore_results, log_genie_processing_failure
from genie.util.common_util import divide_batch_based_on_batch_length, prepare_metadata, generate_bulk_insert_query, datastore_callback, encode_file_name, detect_language,generate_keywords, read_file
from agentbuilder.core import Datastore

#logging custom library import & initialization
from genie import GOOGLE_CLOUD_PROJECT, logger

datastore_processed = 0
data_extraction_ingestion_job_map = {}

class SafeDictParser:
    def __init__(self, dictionary):
        self.dictionary = dictionary

    def parse(self, key):
        return str(self.dictionary.get(key, ''))

class AEM:
    """
    Represents a AEM.
    This class provide basic representation of a sharepoint document ingestion pipeline.
    """
    def __init__(self, config_path):
        """
        Initialize a new instance of AEM.
        Args:
            config_path (str): Path of the config yaml file
        """

        #Initialize all the required configurations of sharepoint and cloud for new instance.
        with open(config_path, 'r') as config_file:
            self.config = yaml.safe_load(config_file)
            self.pillar = self.config['pillar']
            self.project_number = self.config['gcp']['project_number']
            self.datastore_id=self.config['gcp']['datastore_id']
            #each zip file size
            self.batch_size = self.config['batch_size']
            
            #vault config
            self.vault_available = self.config['hashicorp_vault']['available']
            self.vault_namespace = self.config['hashicorp_vault']['namespace']
            
            #Credentials fetch for ETL account
            self.client_id = "cm-p143511-e1511071-integration-0"
            self.client_secret = "pp8e-Ib_9RSAOv5yPJwDtwmiSbhwQirdxIQvl"
            self.auth_url = "https://ims-na1.adobelogin.com/ims/exchange/jwt"
            self.url="https://author-p143511-e1495049.adobeaemcloud.com"
            self.private_key_path = "stage_private.key"
            #self.access_token = self.get_token()

            #initial temporary path for download self.config
            self.created_date = self.updated_date = str(datetime.now()) #unique run time
            self.date = datetime.now().strftime("%Y-%m-%d")
            self.date_string = datetime.now().strftime("%Y%m%d")

            #initialize GCP cloud SQL self.config
            self.db_name = self.config['gcp']['cloudsql']['db_name']
            self.project_id = self.config['gcp']['project_id']
            self.region = self.config['gcp']['cloudsql']['region']
            self.instance_name = self.config['gcp']['cloudsql']['instance_name']
            
            #connection to connect cloudsql instance
            self.alloydb_available = self.config['gcp']['alloydb']['available']
            if self.alloydb_available == 'True':
                self.cluster_name = self.config['gcp']['alloydb']['cluster_name']
                self.instance_connection_name = f"projects/{self.project_id}/locations/{self.region}/clusters/{self.cluster_name}/instances/{self.instance_name}" #connection to connect Alloydb instance
            else:
                self.instance_connection_name = f"{self.project_id}:{self.region}:{self.instance_name}" # connection to connect Cloudsql instance
            self.schema = self.config['gcp']['cloudsql']['tables']['schema']
            self.pubsub_topic = self.config['gcp']['pubsub']['topic']

            #initialize the metadata table names
            self.doc_info_table = self.db_name+'.'+self.schema+ '.'+self.config['gcp']['cloudsql']['tables']['doc_info_table']
            self.search_profile_table = self.db_name+'.'+self.schema+'.'+self.config['gcp']['cloudsql']['tables']['search_profile_table']
            self.doc_type_table = self.db_name+'.'+self.schema+'.'+self.config['gcp']['cloudsql']['tables']['doc_type_table']
            self.job_history_table = self.db_name+'.'+self.schema+'.'+self.config['gcp']['cloudsql']['tables']['job_history_table']
            self.sites_list_table=self.db_name+'.'+self.schema+ '.'+self.config['gcp']['cloudsql']['tables']['sites_list_table']
            self.exclude_folder_filter_table=self.db_name+'.'+self.schema+ '.'+self.config['gcp']['cloudsql']['tables']['exclude_filter_table']
            self.include_folder_filter_table=self.db_name+'.'+self.schema+ '.'+self.config['gcp']['cloudsql']['tables']['include_filter_table']
            self.failure_log_table=self.db_name+'.'+self.schema+ '.'+self.config['gcp']['cloudsql']['tables']['failure_log_table']
            self.unique_key = self.config['gcp']['cloudsql']['tables']['unique_key'] # unique key

            #initialize the GCP Storage details
            self.bucket_name = self.config['gcp']['gcs']['bucket_name']
            self.private_bucket_name = self.config['gcp']['gcs']['private_bucket_name']
            self.metadata_folder = self.config['gcp']['gcs']['metadata_folder']
            self.document_folder = self.config['gcp']['gcs']['document_folder']

            self.all_sites_query=Template(self.config['gcp']['cloudsql']['queries']['get_sites_list_query']).render(sites_list_table_name=self.sites_list_table)
            

            #postgres insert batch size
            self.postgres_insert_batch_size = 20000

    def extract_etl_creds(self,gcp_instance):
        """
        Extracts the database credentials from the Google Cloud Platform Secret Manager.

        Args:
            gcp_instance (GcpUtils): An instance of the GcpUtils class that provides
                methods for interacting with Google Cloud Platform services.

        Returns:
            None
        """
        self.client_id = gcp_instance.get_secret(self.config['gcp']['secret_manager']['etl_client_id'])
        self.client_secret = gcp_instance.get_secret(self.config['gcp']['secret_manager']['etl_client_secret'])
        self.private_key = gcp_instance.get_secret(self.config['gcp']['secret_manager']['etl_private_key'])
        self.jwtPayloadRaw = gcp_instance.get_secret(self.config['gcp']['secret_manager']['etl_jwtPayloadRaw'])
        
    def encode_file_name(self, file_name: str, profile_id: str) -> str:
        """
        Encodes a file name to prevent Google Cloud Storage (GCS) file name errors and includes a profile ID.

        This function takes a file name and a profile ID as input and performs the following transformations:
        1. Extracts the file extension.
        2. Removes leading and trailing underscores from the file name.
        3. Replaces underscores with hyphens in the file name.
        4. Removes carriage return characters from the file name.
        5. Adds the profile ID to the file name.
        6. Reconstructs the file name with the updated extension.
        7. Encodes the file name using the `quote` function to prevent GCS file name errors.

        Args:
            file_name (str): The file name to encode.
            profile_id (str): The profile ID to include in the file name.

        Returns:
            str: The encoded file name.

        Example:
            >>> file_name = "_my_document_with_carriage_return_.pdf"
            >>> profile_id = "12345"
            >>> encoded_file_name = encode_file_name(file_name, profile_id)
            >>> print(encoded_file_name)
            my-document-with-carriage-return-12345.pdf

        """
        file_extension = file_name.split(".")[-1]
        # 1. Extract the file name without extension
        filename = file_name.replace(f".{file_extension}", "")

        # 2. Remove leading and trailing '_' from filename
        filename = filename.strip("_")

        # 3. Replace '_' with '-' in the filename
        filename = filename.replace("_", "-")

        # 4. Remove any carriage return characters
        filename = re.sub(r"[\r\n\t]", "", filename)

        # 5. Add the profile ID to the filename
        filename = f"{filename}-{profile_id}"

        # 6. Reconstruct the filename with updated extension
        encoded_filename = filename + "." + file_extension

        # 7. Encode the file name to prevent GCS file name errors
        encoded_filename = quote(encoded_filename)

        return encoded_filename
            
    def extract_db_creds(self,gcp_instance):
        """
        Extracts the database credentials from the Google Cloud Platform Secret Manager.

        Args:
            gcp_instance (GcpUtils): An instance of the GcpUtils class that provides
                methods for interacting with Google Cloud Platform services.

        Returns:
            None
        """
        self.db_user = gcp_instance.get_secret(self.config['gcp']['secret_manager']['cloudsql_username'])
        self.db_pass = gcp_instance.get_secret(self.config['gcp']['secret_manager']['cloudsql_password'])
    def callback_trigger(self, operation):
        """
        Callback function to log Datastore ingestion results.

        This function is called by the Datastore client after each batch of documents is processed.
        It logs the results of the ingestion operation, including success and failure counts, to the SQL tables.

        Args:
            operation: The Datastore operation object.
        """
        global datastore_processed
        global data_extraction_ingestion_job_map
        out = datastore_callback(operation)
        batch_insert_status,datastore_failure_insert_status = log_datastore_results(out, data_extraction_ingestion_job_map, self)
        if batch_insert_status and datastore_failure_insert_status:
            datastore_processed+=1
        else:
            logger.error(f"Unable to update {self.genie_data_store_batch_history_table} table or {self.genie_error_log_table}")

        logger.info(f"Processed Requests: {datastore_processed}")       
    def escape_chars(self,data):
        """
        replace the characters to overcome issues while PSQL insert
        Args:
            data (str): string data that need cleansing as per SQL insert statment compatibility 
        Return:
            cleansed string
        ##Observed $ and ' during initial development - this can be customized as per need basis
        """
        return data.replace('$','$$').replace("'","''").replace(':','\\:') if data and type(data) != int else data

    def generate_bulk_insert_with_merge_query(self, table_name, data_list, unique_key):
        """
        Generate UPSERT/ MERGE statement from the dictionary and table name received
        Args:
            table_name (str): Table name to generate upsert statement
            data_list (list(dict)): List of dictionary with each dictionary having keys as column names and values as column values
            unique_key (str): column to define update in upsert during conflict
        Return:
            SQL UPSERT/ Merge Query
        """
        columns = data_list[0].keys() # read column names
        exclude_columns = [column for column in columns if column != "created_at"] # exclude custom columns that doesn't require update

        #list of values
        values_list = [', '.join(f"'{self.escape_chars(data_dict[col])}'" for col in columns) for data_dict in data_list] 
        update_values = [
            f"{col} = EXCLUDED.{col}"
            for col in exclude_columns
        ]
        #form the SQL UPSERT query                                                                                                               
        insert_query = f"""
            INSERT INTO {table_name} ({', '.join(columns)})
            VALUES ({'), ('.join(values_list)})
            ON CONFLICT ({unique_key})
            DO UPDATE SET {', '.join(update_values)}"""
        return insert_query

    def generate_bulk_insert_query(self, table_name, data_list):
        """
        Generate INSERT statement from the dictionary and table name received
        Args:
            table_name (str): Table name to generate upsert statement
            data_list (list(dict)): List of dictionary with each dictionary having keys as column names and values as column values
            unique_key (str): column to define update in upsert during conflict
        Return:
            SQL INSERT Query
        """
        columns = data_list[0].keys() # read column names
        exclude_columns = [column for column in columns if column != "created_at"] # exclude custom columns that doesn't require update

        #list of values
        values_list = [', '.join(f"'{self.escape_chars(data_dict[col])}'" for col in columns) for data_dict in data_list] 

        #form the SQL UPSERT query                                                                                                               
        insert_query = f"""
            INSERT INTO {table_name} ({', '.join(columns)})
            VALUES ({'), ('.join(values_list)})"""
        return insert_query
    
    def define_delta_date_query(self,name):
        """
        prepares a query to extract the last execution date for each site received iteratively
        """
        self.delta_date_query = Template(self.config['gcp']['cloudsql']['queries']['get_delta_date_query']).render(search_profile_table=self.search_profile_table,
                                                                                                       doc_info_table=self.doc_info_table,
                                                                                                       site_url=name)

    def define_history_insert_query(self,name):
        """
        Prepare query to insert data into History table
        """
        self.job_insert = Template(self.config['gcp']['cloudsql']['queries']['get_history_insert_query']).render(search_profile_table=self.search_profile_table,
                                                                                                   doc_info_table=self.doc_info_table,
                                                                                                   history_table=self.job_history_table,
                                                                                                   update_date=self.updated_date,
                                                                                                   site_url=name)

    def define_metadata_query(self,name):
        """
        prepare query to extract metadata
        """
        self.metadata_query = Template(self.config['gcp']['cloudsql']['queries']['get_metadata_query']).render(search_profile_table=self.search_profile_table,
                                                                                                   doc_info_table=self.doc_info_table,
                                                                                                   doc_type_table=self.doc_type_table,
                                                                                                   history_table=self.job_history_table,
                                                                                                   exclude_filter_table = self.exclude_folder_filter_table,
                                                                                                   include_filter_table = self.include_folder_filter_table,
                                                                                                   batch_size=self.batch_size,
                                                                                                   site_url=name)

    
    

    def parse_log_result_job_history(self,result_row,code,message,file_name):
        """
        parse the data and form a dictionary with values required for job_history table
        Args:
            metadata (dict): file properties metadata
        Return:
            dictionary with keys as column names and values as column values for job_history table
        """
        
        data ={
                'profile_id': result_row['profile_id'],
                'type_id': '0',
                'doc_id': result_row['doc_id'],
                'job_id': file_name,
                'status_code':code,
                'status_message': message,
                'created_at': str(datetime.now())
            }
        return data

    def prepare_ml_trigger(self,metadata_path,run_id):
        """
        module to prepare ml trigger message
        """
        data = {
                    "job_id": str(run_id),
                    "date": str(self.date),
                    "source_type": "aem",
                    "metadata_json_paths":metadata_path,
                    "delete_flag": False
                }
        return data
    
    @retry(delay=2, tries=1, backoff=2)
    def download_file(self, url,aem_instance, instance_type):
        """
        download the file from EDC API
        Args:
            data (dict): dictionary with information of the file to download
        Retrun:
            response of the download
        """
        
        try:
            if instance_type=='AEM_DAM':
                headers = {"Authorization": f"Bearer {self.access_token}"}
                response = requests.get(url,  headers=headers)
                response.raise_for_status()
            elif instance_type=='AEM_CPG':
                response = requests.get(url, auth=(aem_instance.username, aem_instance.password))
                response.raise_for_status()
        except Exception as file_exception:
            logger.error("Failed to download the file, Error as below")
            logger.error(file_exception,exc_info=True)
            logger.error("Retrying again to download by resetting the connection")
            raise
        return response

    def download_batch_files(self, batch_data, job_id, aem_instance, gcp_instance):
        """
        download the batch of files from EDC API and upload to gcs bucket
        Args:
            batch_data (list): list of files to download from the API
        Return:
            successfull download log details
        """
        instance_type= aem_instance.__class__.__name__
        history_log = [] #initialize empty list to update the history log table
        for data in batch_data:
            file_name = self.encode_file_name(data['file_name'],data['profile_id']) # derive file name
            file_path = os.path.join(self.document_folder, self.date, file_name) # derive file path
            try:
                download_url = data['server_redirected_url']
                response = self.download_file(download_url,aem_instance, instance_type)
                
                if response.status_code == 200 and len(response.content) > 0:
                    res = False
                    res = gcp_instance.write_binary_to_gcs(response.content, file_path)
                    if res:
                        history_log.append(self.parse_log_result_job_history(data,'1','Download Successful', job_id))
                        logger.info("file loaded into GCS successfully: %s",file_path)
                    else:
                        history_log.append(self.parse_log_result_job_history(data,'2','Upload Failed', job_id)) #log to ingest failure
                        logger.info("file load into GCS Failed: %s",file_path)
                elif response.status_code == 200 and len(response.content) == 0:
                    history_log.append(self.parse_log_result_job_history(data,'2','Empty file', job_id))
                else:
                    history_log.append(self.parse_log_result_job_history(data,'2',response.reason, job_id)) #log to ingest failure
            except Exception as file_exception:
                logger.error("Failed to download even after retry")
                logger.error(file_exception,exc_info=True)
                history_log.append(self.parse_log_result_job_history(data,'2','EXCEPTION OCCURRED: FAILED TO DOWNLOAD', job_id))
                continue
        return history_log
    
    def get_token(self):
        url = 'https://ims-na1.adobelogin.com/ims/exchange/jwt'
        jwtPayloadRaw = self.jwtPayloadRaw
        jwtPayloadJson = json.loads(jwtPayloadRaw)
        jwtPayloadJson["exp"] = datetime.utcnow() + \
            timedelta(seconds=1000)

        accessTokenRequestPayload = {
            'client_id': self.client_id, 'client_secret': self.client_secret}

        
        private_key = self.private_key
        print("private_key:",private_key)
         
        jwttoken = jwt.encode(jwtPayloadJson, private_key, algorithm='RS256')

        accessTokenRequestPayload['jwt_token'] = jwttoken
        #print(url)
        #print(accessTokenRequestPayload)
        result = requests.post(url, data=accessTokenRequestPayload)
        print("result:",result)
        resultjson = json.loads(result.text)

        
        return resultjson["access_token"]

class AEM_DAM:

    def __init__(self, aem_instance,gcp_instance):
        self.aem_instance = aem_instance
        self.aem_base_url = self.aem_instance.config['aem_dam']['base_url']
        self.filter_query = Template(self.aem_instance.config['aem_dam']['api_filter']).render(aem_url=self.aem_base_url)

        self.username = gcp_instance.get_secret(self.aem_instance.config['gcp']['secret_manager']['generic_account_aem_dam_username'])
        if self.aem_instance.vault_available == "True":
            self.password = HashiCorpVault(self.aem_instance.vault_namespace, self.username).get_password()
        else:
            self.password = gcp_instance.get_secret(self.aem_instance.config['gcp']['secret_manager']['generic_account_aem_dam_password'])


    @retry(delay=2, tries=3, backoff=2)
    def generate_metadata_file(self, doc_info, search_profile, aem_folder, last_modified_date):
        """
        Extract the delta metadata from AEM API
        Args:
            doc_info (dict): dictionary to store medata for doc_info table
            search_profile (dict): dictionary to store medata for search_profile table
            filter_query (str): Query to fetch delta from the API
        Return:
            Dictionaries with metadata extracted
        """
        
        arm_url = self.filter_query.format(aem_folder,last_modified_date,datetime.utcnow().isoformat())
        headers = {"Authorization": f"Bearer {self.aem_instance.access_token}"}
        response = requests.get(arm_url, headers=headers)
        response.raise_for_status()

        dataframe = pd.json_normalize(response.json().get("hits"))

        df_filtered = dataframe.drop_duplicates(subset='jcr:content.mt:documentID', keep=False)
        
        data = json.loads(df_filtered.to_json(orient='records'))
        for d in data:
            doc_info['doc_info_metadata'].append(self.parse_metadata_result_doc_info(d,aem_folder))
            search_profile['search_profile_metadata'].append(self.parse_metadata_result_search_profile(d))
        aem_data = {aem_d['jcr:content.mt:documentID']: aem_d for aem_d in data}
        return aem_data

    def parse_metadata_result_doc_info(self,metadata,aem_loc):
        """
        parse the data and form a dictionary with values required for doc_info table
        Args:
            metadata (dict): file properties metadata
        Return:
            dictionary with keys as column names and values as column values for doc_info table
        """
        parser = SafeDictParser(metadata)
        
        data ={
                'doc_id': parser.parse('jcr:content.mt:documentID'),
                'doc_type': 'aem-dam',
                'file_name': remove_non_printable(os.path.basename("https://www.micron.com" + parser.parse('jcr:path')) if (parser.parse('jcr:content.cq:name') is None or parser.parse('jcr:content.cq:name') =='None') else parser.parse('jcr:content.cq:name')).replace('_', '-').replace('/', '-').replace('\r\n', ''),
                'filesize_mb': '',
                'author': str(parser.parse('jcr:content.metadata.dc:creator')),
                'url': "https://www.micron.com" + parser.parse('jcr:path'),
                'server_redirected_url': self.aem_base_url + parser.parse('jcr:path'),
                'server_redirected_preview_url': "https://www.micron.com" + parser.parse('jcr:path'),
                'file_type': 'pdf' if parser.parse('jcr:content.metadata.dc:format') =='application/pdf' else
                             'docx' if parser.parse('jcr:content.metadata.dc:format') == 'application/vnd.openxmlformats-officedocument.wordprocessingml.document' else
                             'pptx' if parser.parse('jcr:content.metadata.dc:format') == 'application/vnd.openxmlformats-officedocument.presentationml.presentation' else
                             parser.parse('jcr:content.metadata.dc:format'),
                'site_name': aem_loc,
                'lifetime_views': '',
                'recent_views': '',
                'created_at': self.aem_instance.created_date,
                'updated_at': self.aem_instance.updated_date,
                'last_modified_time': '1900-01-01 00:00:00' if (parser.parse('jcr:content.cq:lastReplicated') == 'None' or parser.parse('jcr:content.cq:lastReplicated') == '') else datetime.strptime(parser.parse('jcr:content.cq:lastReplicated'), '%a %b %d %Y %H:%M:%S GMT%z').strftime('%Y-%m-%d %H:%M:%S')
            }
        return data

    def parse_metadata_result_search_profile(self,metadata):
        """
        parse the data and form a dictionary with values required for search_profile table
        Args:
            metadata (dict): file properties metadata
        Return:
            dictionary with keys as column names and values as column values for search_profile table
        """
        parser = SafeDictParser(metadata)

        data ={
                'type_id': 0,
                'doc_id': parser.parse('jcr:content.mt:documentID'),
                'doc_name': remove_non_printable(os.path.basename("https://www.micron.com" + parser.parse('jcr:path')) if (parser.parse('jcr:content.cq:name') is None or parser.parse('jcr:content.cq:name') =='None') else parser.parse('jcr:content.cq:name')).replace('_', '-').replace('/', '-').replace('\r\n', ''),
                'doc_url': "https://www.micron.com" + parser.parse('jcr:path'),
                'last_modified_time': '1900-01-01 00:00:00' if (parser.parse('jcr:content.cq:lastReplicated') == 'None' or parser.parse('jcr:content.cq:lastReplicated') == '') else datetime.strptime(parser.parse('jcr:content.cq:lastReplicated'), '%a %b %d %Y %H:%M:%S GMT%z').strftime('%Y-%m-%d %H:%M:%S'),
                'created_at': self.aem_instance.created_date,
                'updated_at': self.aem_instance.updated_date
            }
        return data
    def callback_trigger(self, operation):
        """
        Callback function to log Datastore ingestion results.

        This function is called by the Datastore client after each batch of documents is processed.
        It logs the results of the ingestion operation, including success and failure counts, to the SQL tables.

        Args:
            operation: The Datastore operation object.
        """
        global datastore_processed
        global data_extraction_ingestion_job_map
        out = datastore_callback(operation)
        batch_insert_status,datastore_failure_insert_status = log_datastore_results(out, data_extraction_ingestion_job_map, self)
        if batch_insert_status and datastore_failure_insert_status:
            datastore_processed+=1
        else:
            logger.error(f"Unable to update {self.genie_data_store_batch_history_table} table or {self.genie_error_log_table}")

        logger.info(f"Processed Requests: {datastore_processed}")
    def process_metadata(self,rows,aem_data):
        """
        Processes the metadata file to support the Vertex AI Search API
        """
        result_medata = []

        for row in rows:
            if row['doc_id'] in aem_data:
                data = aem_data[row['doc_id']]
                parser = SafeDictParser(data)

                json_data = {
                    # --- MODIFICATION START ---
                    'doc_type' : 'aem-dam',
                    'file_name' : remove_non_printable(os.path.basename("https://www.micron.com" + parser.parse('jcr:path')) if (parser.parse('jcr:content.cq:name') is None or parser.parse('jcr:content.cq:name') =='None') else parser.parse('jcr:content.cq:name')).replace('_', '-').replace('/', '-').replace('\r\n', ''),
                    'file_name_keyprop': remove_non_printable(os.path.basename("https://www.micron.com" + parser.parse('jcr:path')) if (parser.parse('jcr:content.cq:name') is None or parser.parse('jcr:content.cq:name') =='None') else parser.parse('jcr:content.cq:name')).replace('_', '-').replace('/', '-').replace('\r\n', ''),
                    'url' : remove_non_printable("https://www.micron.com" + parser.parse('jcr:path')),
                    'pillar' : remove_non_printable('sales'),
                    'language' : remove_non_printable("{'English': 99.0}"),
                    'deleted' : remove_non_printable('no'),
                    'author' : '' if (parser.parse('jcr:content.metadata.dc:creator') is None or parser.parse('jcr:content.metadata.dc:creator').lower()=="none") else remove_non_printable(parser.parse('jcr:content.metadata.dc:creator')),
                    'last_modified_time': remove_non_printable( '1900-01-01 00:00:00' if (parser.parse('jcr:content.mt:lastModified') == 'None' or parser.parse('jcr:content.mt:lastModified') == '') else datetime.strptime(parser.parse('jcr:content.mt:lastModified'), '%a %b %d %Y %H:%M:%S GMT%z').strftime('%Y-%m-%d %H:%M:%S') ),
                    'title' : remove_non_printable(parser.parse('jcr:content.metadata.jcr:title')).replace('_', '-').replace('/', '-').replace('\r\n', ''),
                    'file_type' : remove_non_printable(parser.parse('jcr:content.metadata.dc:format')),
                    'doc_id' : remove_non_printable(parser.parse('jcr:content.mt:documentID')),
                    # 'last_replicated' : remove_non_printable(parser.parse('jcr:content.cq:lastReplicated')),
                    # 'last_replicated_action' : remove_non_printable(parser.parse('jcr:content.cq:lastReplicationAction')),
                    'aem_object_id' : remove_non_printable(parser.parse('jcr:content.mt:documentID')),
                    #'publisher' : remove_non_printable(parser.parse('jcr:createdBy')),
                    'aem_uuid' : remove_non_printable(parser.parse('jcr:uuid')),
                    #'published_content_last_modified_by' : remove_non_printable(parser.parse('jcr:content.jcr:lastModifiedBy')),
                    #'published_content_last_modified_time': remove_non_printable(parser.parse('jcr:content.jcr:lastModified')),
                    # 'legacy_id' : remove_non_printable(parser.parse('jcr:content.mt:legacyID')),
                    'doc_category' : parse_aem_doc_category(
                        simplify_text( remove_non_printable( parser.parse('jcr:content.metadata.mt:contentType') ).lower().replace(',', ' ').replace('-', ' ').replace('–', ' ').strip() ),
                        remove_non_printable("https://www.micron.com" + parser.parse('jcr:path'))
                    ).strip(),
                    'last_modified_unix_time' : int(row['last_modified_unix_time']),
                    'last_modified_iso_time': datetime.fromtimestamp(int(row['last_modified_unix_time']), tz=timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ'),
                    'description' : ' '.join([
                        simplify_text( remove_non_printable( parser.parse('jcr:content.metadata.dc:description')).lower().replace(',', ' ').replace('-', ' ').replace('–', ' ').strip() ),
                    ]).strip(),
                    'subject' : ' '.join([
                        parse_subject( remove_non_printable( parser.parse('jcr:content.metadata.dc:subject') ).lower() ),
                    ]).strip(),
                    'products' : ' '.join( parse_products( remove_non_printable( parser.parse('jcr:content.metadata.mt:techProductNames') ).lower() ) ),
                    'optional_text_field_1' : ' '.join([
                        simplify_text( remove_non_printable( parser.parse('jcr:content.metadata.pdfx:BookLibrary') ).lower().replace(',', ' ').replace('-', ' ').replace('–', ' ').strip() ),
                    ]).strip(),
                    'optional_text_field_2' : ' '.join([
                        simplify_text( remove_non_printable( parser.parse('jcr:content.metadata.pdfx:BookTitleAlt1') ).lower().replace(',', ' ').replace('-', ' ').replace('–', ' ').strip() ),
                    ]).strip(),
                    'optional_text_field_3' : ' '.join([
                        simplify_text( remove_non_printable( parser.parse('jcr:content.metadata.pdfx:BM_ProdName') ).lower().replace(',', ' ').replace('-', ' ').replace('–', ' ').strip() ),
                    ]).strip(),
                    'optional_text_field_4' : ' '.join([
                        '',
                    ]).strip(),
                    'optional_text_field_5' : ' '.join([
                        '',
                    ]).strip(),

                     # NER metadata.
                    'business_unit_list' : build_ner_data([parser.parse('jcr:content.metadata.mt:rba')], 'bu'),
                    'market_subsegment_list' : build_ner_data(
                        [
                            remove_non_printable(os.path.basename("https://www.micron.com" + parser.parse('jcr:path')) if parser.parse('jcr:content.cq:name') == 'None' else parser.parse('jcr:content.cq:name')),
                            remove_non_printable("https://www.micron.com" + parser.parse('jcr:path')),
                            ' '.join([simplify_text( remove_non_printable( parser.parse('jcr:content.metadata.dc:description')).lower().replace(',', ' ').replace('-', ' ').replace('–', ' ').strip() )]),
                            ' '.join([parse_subject( remove_non_printable( parser.parse('jcr:content.metadata.dc:subject') ).lower() )]),
                            ' '.join( parse_products( remove_non_printable( parser.parse('jcr:content.metadata.mt:techProductNames') ).lower() ) ),
                            ' '.join([simplify_text( remove_non_printable( parser.parse('jcr:content.metadata.pdfx:BookLibrary') ).lower().replace(',', ' ').replace('-', ' ').replace('–', ' ').strip() )]),
                            ' '.join([simplify_text( remove_non_printable( parser.parse('jcr:content.metadata.pdfx:BookTitleAlt1') ).lower().replace(',', ' ').replace('-', ' ').replace('–', ' ').strip() )]),
                            ' '.join([simplify_text( remove_non_printable( parser.parse('jcr:content.metadata.pdfx:BM_ProdName') ).lower().replace(',', ' ').replace('-', ' ').replace('–', ' ').strip() )]),
                        ],
                        'market_subsegment',
                    ),
                    'mpn_list': build_ner_data(
                        [
                            remove_non_printable(os.path.basename("https://www.micron.com" + parser.parse('jcr:path')) if parser.parse('jcr:content.cq:name') == 'None' else parser.parse('jcr:content.cq:name')),
                            remove_non_printable("https://www.micron.com" + parser.parse('jcr:path')),
                            ' '.join([simplify_text( remove_non_printable( parser.parse('jcr:content.metadata.dc:description')).lower().replace(',', ' ').replace('-', ' ').replace('–', ' ').strip() )]),
                            ' '.join([parse_subject( remove_non_printable( parser.parse('jcr:content.metadata.dc:subject') ).lower() )]),
                            ' '.join( parse_products( remove_non_printable( parser.parse('jcr:content.metadata.mt:techProductNames') ).lower() ) ),
                            ' '.join([simplify_text( remove_non_printable( parser.parse('jcr:content.metadata.pdfx:BookLibrary') ).lower().replace(',', ' ').replace('-', ' ').replace('–', ' ').strip() )]),
                            ' '.join([simplify_text( remove_non_printable( parser.parse('jcr:content.metadata.pdfx:BookTitleAlt1') ).lower().replace(',', ' ').replace('-', ' ').replace('–', ' ').strip() )]),
                            ' '.join([simplify_text( remove_non_printable( parser.parse('jcr:content.metadata.pdfx:BM_ProdName') ).lower().replace(',', ' ').replace('-', ' ').replace('–', ' ').strip() )]),
                        ],
                        'mpn',
                    ),
                    'design_id_list': build_ner_data(
                        [
                            remove_non_printable(os.path.basename("https://www.micron.com" + parser.parse('jcr:path')) if parser.parse('jcr:content.cq:name') == 'None' else parser.parse('jcr:content.cq:name')),
                            remove_non_printable("https://www.micron.com" + parser.parse('jcr:path')),
                            ' '.join([simplify_text( remove_non_printable( parser.parse('jcr:content.metadata.dc:description')).lower().replace(',', ' ').replace('-', ' ').replace('–', ' ').strip() )]),
                            ' '.join([parse_subject( remove_non_printable( parser.parse('jcr:content.metadata.dc:subject') ).lower() )]),
                            ' '.join( parse_products( remove_non_printable( parser.parse('jcr:content.metadata.mt:techProductNames') ).lower() ) ),
                            ' '.join([simplify_text( remove_non_printable( parser.parse('jcr:content.metadata.pdfx:BookLibrary') ).lower().replace(',', ' ').replace('-', ' ').replace('–', ' ').strip() )]),
                            ' '.join([simplify_text( remove_non_printable( parser.parse('jcr:content.metadata.pdfx:BookTitleAlt1') ).lower().replace(',', ' ').replace('-', ' ').replace('–', ' ').strip() )]),
                            ' '.join([simplify_text( remove_non_printable( parser.parse('jcr:content.metadata.pdfx:BM_ProdName') ).lower().replace(',', ' ').replace('-', ' ').replace('–', ' ').strip() )]),
                        ],
                        'design_id',
                    ),
                    'functional_technology_list': build_ner_data(
                        [
                            remove_non_printable(os.path.basename("https://www.micron.com" + parser.parse('jcr:path')) if parser.parse('jcr:content.cq:name') == 'None' else parser.parse('jcr:content.cq:name')),
                            remove_non_printable("https://www.micron.com" + parser.parse('jcr:path')),
                            ' '.join([simplify_text( remove_non_printable( parser.parse('jcr:content.metadata.dc:description')).lower().replace(',', ' ').replace('-', ' ').replace('–', ' ').strip() )]),
                            ' '.join([parse_subject( remove_non_printable( parser.parse('jcr:content.metadata.dc:subject') ).lower() )]),
                            ' '.join( parse_products( remove_non_printable( parser.parse('jcr:content.metadata.mt:techProductNames') ).lower() ) ),
                            ' '.join([simplify_text( remove_non_printable( parser.parse('jcr:content.metadata.pdfx:BookLibrary') ).lower().replace(',', ' ').replace('-', ' ').replace('–', ' ').strip() )]),
                            ' '.join([simplify_text( remove_non_printable( parser.parse('jcr:content.metadata.pdfx:BookTitleAlt1') ).lower().replace(',', ' ').replace('-', ' ').replace('–', ' ').strip() )]),
                            ' '.join([simplify_text( remove_non_printable( parser.parse('jcr:content.metadata.pdfx:BM_ProdName') ).lower().replace(',', ' ').replace('-', ' ').replace('–', ' ').strip() )]),
                        ],
                        'functional_technology',
                    ),
                    'package_type_list': build_ner_data(
                        [
                            remove_non_printable(os.path.basename("https://www.micron.com" + parser.parse('jcr:path')) if parser.parse('jcr:content.cq:name') == 'None' else parser.parse('jcr:content.cq:name')),
                            remove_non_printable("https://www.micron.com" + parser.parse('jcr:path')),
                            ' '.join([simplify_text( remove_non_printable( parser.parse('jcr:content.metadata.dc:description')).lower().replace(',', ' ').replace('-', ' ').replace('–', ' ').strip() )]),
                            ' '.join([parse_subject( remove_non_printable( parser.parse('jcr:content.metadata.dc:subject') ).lower() )]),
                            ' '.join( parse_products( remove_non_printable( parser.parse('jcr:content.metadata.mt:techProductNames') ).lower() ) ),
                            ' '.join([simplify_text( remove_non_printable( parser.parse('jcr:content.metadata.pdfx:BookLibrary') ).lower().replace(',', ' ').replace('-', ' ').replace('–', ' ').strip() )]),
                            ' '.join([simplify_text( remove_non_printable( parser.parse('jcr:content.metadata.pdfx:BookTitleAlt1') ).lower().replace(',', ' ').replace('-', ' ').replace('–', ' ').strip() )]),
                            ' '.join([simplify_text( remove_non_printable( parser.parse('jcr:content.metadata.pdfx:BM_ProdName') ).lower().replace(',', ' ').replace('-', ' ').replace('–', ' ').strip() )]),
                        ],
                        'package_type',
                    ),
                     'density_list': build_ner_data(
                        [
                            remove_non_printable(os.path.basename("https://www.micron.com" + parser.parse('jcr:path')) if parser.parse('jcr:content.cq:name') == 'None' else parser.parse('jcr:content.cq:name')),
                            remove_non_printable("https://www.micron.com" + parser.parse('jcr:path')),
                            ' '.join([simplify_text( remove_non_printable( parser.parse('jcr:content.metadata.dc:description')).lower().replace(',', ' ').replace('-', ' ').replace('–', ' ').strip() )]),
                            ' '.join([parse_subject( remove_non_printable( parser.parse('jcr:content.metadata.dc:subject') ).lower() )]),
                            ' '.join( parse_products( remove_non_printable( parser.parse('jcr:content.metadata.mt:techProductNames') ).lower() ) ),
                            ' '.join([simplify_text( remove_non_printable( parser.parse('jcr:content.metadata.pdfx:BookLibrary') ).lower().replace(',', ' ').replace('-', ' ').replace('–', ' ').strip() )]),
                            ' '.join([simplify_text( remove_non_printable( parser.parse('jcr:content.metadata.pdfx:BookTitleAlt1') ).lower().replace(',', ' ').replace('-', ' ').replace('–', ' ').strip() )]),
                            ' '.join([simplify_text( remove_non_printable( parser.parse('jcr:content.metadata.pdfx:BM_ProdName') ).lower().replace(',', ' ').replace('-', ' ').replace('–', ' ').strip() )]),
                        ],
                        'density',
                    ),
                    'product_series_list': build_ner_data(
                        [
                            remove_non_printable(os.path.basename("https://www.micron.com" + parser.parse('jcr:path')) if parser.parse('jcr:content.cq:name') == 'None' else parser.parse('jcr:content.cq:name')),
                            remove_non_printable("https://www.micron.com" + parser.parse('jcr:path')),
                            ' '.join([simplify_text( remove_non_printable( parser.parse('jcr:content.metadata.dc:description')).lower().replace(',', ' ').replace('-', ' ').replace('–', ' ').strip() )]),
                            ' '.join([parse_subject( remove_non_printable( parser.parse('jcr:content.metadata.dc:subject') ).lower() )]),
                            ' '.join( parse_products( remove_non_printable( parser.parse('jcr:content.metadata.mt:techProductNames') ).lower() ) ),
                            ' '.join([simplify_text( remove_non_printable( parser.parse('jcr:content.metadata.pdfx:BookLibrary') ).lower().replace(',', ' ').replace('-', ' ').replace('–', ' ').strip() )]),
                            ' '.join([simplify_text( remove_non_printable( parser.parse('jcr:content.metadata.pdfx:BookTitleAlt1') ).lower().replace(',', ' ').replace('-', ' ').replace('–', ' ').strip() )]),
                            ' '.join([simplify_text( remove_non_printable( parser.parse('jcr:content.metadata.pdfx:BM_ProdName') ).lower().replace(',', ' ').replace('-', ' ').replace('–', ' ').strip() )]),
                        ],
                        'product_series',
                    ),
                    'process_requirement_list': build_ner_data(
                         [
                            remove_non_printable(os.path.basename("https://www.micron.com" + parser.parse('jcr:path')) if parser.parse('jcr:content.cq:name') == 'None' else parser.parse('jcr:content.cq:name')),
                            remove_non_printable("https://www.micron.com" + parser.parse('jcr:path')),
                            ' '.join([simplify_text( remove_non_printable( parser.parse('jcr:content.metadata.dc:description')).lower().replace(',', ' ').replace('-', ' ').replace('–', ' ').strip() )]),
                            ' '.join([parse_subject( remove_non_printable( parser.parse('jcr:content.metadata.dc:subject') ).lower() )]),
                            ' '.join( parse_products( remove_non_printable( parser.parse('jcr:content.metadata.mt:techProductNames') ).lower() ) ),
                            ' '.join([simplify_text( remove_non_printable( parser.parse('jcr:content.metadata.pdfx:BookLibrary') ).lower().replace(',', ' ').replace('-', ' ').replace('–', ' ').strip() )]),
                            ' '.join([simplify_text( remove_non_printable( parser.parse('jcr:content.metadata.pdfx:BookTitleAlt1') ).lower().replace(',', ' ').replace('-', ' ').replace('–', ' ').strip() )]),
                            ' '.join([simplify_text( remove_non_printable( parser.parse('jcr:content.metadata.pdfx:BM_ProdName') ).lower().replace(',', ' ').replace('-', ' ').replace('–', ' ').strip() )]),
                        ],
                        'process_requirement',
                    ),
                }
                
                
                json_data['title']= json_data['title'].replace("_","-").replace("/","-").replace("\r\n","")
                json_data['file_name']= json_data['file_name'].replace("_","-").replace("/","-").replace("\r\n","")
                json_data['file_name_keyprop']= json_data['file_name_keyprop'].replace("_","-").replace("/","-").replace("\r\n","")
            
                m_dict = {
                    "id": "doc-" + str(row['profile_id']),
                    "jsonData": json_data,
                    "content": {
                        "mimeType": "application/pdf" if row['file_type'].lower() == 'pdf' else 
                        "application/vnd.openxmlformats-officedocument.presentationml.presentation" if (row['file_type'].lower() == 'pptx' or row['file_type'].lower() == 'ppt') else
                        "application/vnd.openxmlformats-officedocument.wordprocessingml.document" if (row['file_type'].lower() == 'docx' or row['file_type'].lower() == 'doc') else
                        "text/html" if (row['file_type'].lower() =='htm' or row['file_type'].lower() == 'html') else
                        row['file_type'],
                        "uri": "gs://" + self.aem_instance.private_bucket_name + '/' + self.aem_instance.document_folder + self.aem_instance.date + '/' +self.aem_instance.encode_file_name(row['file_name'],row['profile_id']) 
                    }
                }
                
        
               
                if m_dict['jsonData']['title'].lower() == "none" :
                   m_dict['jsonData']['title'] = m_dict['jsonData']['file_name']
                m_dict['jsonData']['last_modified_unix_time'] = int(float(m_dict['jsonData']['last_modified_unix_time']))
                m_dict['jsonData'] = json.dumps(m_dict['jsonData'])
                json.dumps(result_medata.append(m_dict),ensure_ascii=False)
        return result_medata

class AEM_CPG:

    def __init__(self, aem_instance, gcp_instance):
        self.aem_instance = aem_instance
        self.gcp_instance = gcp_instance
        self.aem_base_url = self.aem_instance.config['aem_cpg']['base_url']
        self.filter_query = Template(self.aem_instance.config['aem_cpg']['api_filter']).render(aem_url=self.aem_base_url)

        self.username = gcp_instance.get_secret(self.aem_instance.config['gcp']['secret_manager']['generic_account_aem_cpg_username'])
        if self.aem_instance.vault_available == "True":
            self.password = HashiCorpVault(self.aem_instance.vault_namespace, self.username).get_password()
        else:
            self.password = gcp_instance.get_secret(self.aem_instance.config['gcp']['secret_manager']['generic_account_aem_cpg_password'])
        

    @retry(delay=2, tries=3, backoff=2)
    def generate_metadata_file(self, doc_info, search_profile, aem_folder, last_modified_date):
        """
        Extract the delta metadata from AEM API
        Args:
            doc_info (dict): dictionary to store medata for doc_info table
            search_profile (dict): dictionary to store medata for search_profile table
            filter_query (str): Query to fetch delta from the API
        Return:
            Dictionaries with metadata extracted
        """
        
        arm_url = self.filter_query.format(aem_folder, last_modified_date, datetime.utcnow().isoformat())
        
        response = requests.get(arm_url, auth=(self.username, self.password))
        response.raise_for_status()

        dataframe = pd.json_normalize(response.json().get("hits"))
        
        df_filtered = dataframe.drop_duplicates(subset='jcr:uuid', keep=False)
        
        data = json.loads(df_filtered.to_json(orient='records'))
        for d in data:
            doc_info['doc_info_metadata'].append(self.parse_metadata_result_doc_info(d,aem_folder))
            search_profile['search_profile_metadata'].append(self.parse_metadata_result_search_profile(d))
        aem_data = {aem_d['jcr:uuid']: aem_d for aem_d in data}
        return aem_data

    def parse_metadata_result_doc_info(self,metadata,aem_loc):
        """
        parse the data and form a dictionary with values required for doc_info table
        Args:
            metadata (dict): file properties metadata
        Return:
            dictionary with keys as column names and values as column values for doc_info table
        """
        parser = SafeDictParser(metadata)
        
        data ={
                'doc_id': parser.parse('jcr:uuid'),
                'doc_type': 'aem-cpg',
                'file_name': remove_non_printable(os.path.basename("https://www.crucial.com" + parser.parse('jcr:path')) if (parser.parse('jcr:content.cq:name') is None or parser.parse('jcr:content.cq:name') =='None') else parser.parse('jcr:content.cq:name')).replace('_', '-').replace('/', '-').replace('\r\n', ''),
                'filesize_mb': '',
                'author': str(parser.parse('jcr:content.metadata.dc:creator')),
                'url': "https://www.crucial.com" + parser.parse('jcr:path'),
                'server_redirected_url': self.aem_base_url + parser.parse('jcr:path'),
                'server_redirected_preview_url': "https://www.crucial.com" + parser.parse('jcr:path'),
                'file_type': 'pdf' if parser.parse('jcr:content.metadata.dc:format') =='application/pdf' else
                             'docx' if parser.parse('jcr:content.metadata.dc:format') == 'application/vnd.openxmlformats-officedocument.wordprocessingml.document' else
                             'pptx' if parser.parse('jcr:content.metadata.dc:format') == 'application/vnd.openxmlformats-officedocument.presentationml.presentation' else
                             'html' if parser.parse('jcr:content.metadata.dc:format') == 'text/html' else
                             parser.parse('jcr:content.metadata.dc:format'),
                'site_name': aem_loc,
                'lifetime_views': '',
                'recent_views': '',
                'created_at': self.aem_instance.created_date,
                'updated_at': self.aem_instance.updated_date,
                'last_modified_time': '1900-01-01 00:00:00' if (parser.parse('jcr:content.cq:lastReplicated') is None or parser.parse('jcr:content.cq:lastReplicated') == '') else datetime.strptime(parser.parse('jcr:content.cq:lastReplicated'), '%a %b %d %Y %H:%M:%S GMT%z').strftime('%Y-%m-%d %H:%M:%S')
            }
        return data

    def parse_metadata_result_search_profile(self,metadata):
        """
        parse the data and form a dictionary with values required for search_profile table
        Args:
            metadata (dict): file properties metadata
        Return:
            dictionary with keys as column names and values as column values for search_profile table
        """
        parser = SafeDictParser(metadata)

        data ={
                'type_id': 0,
                'doc_id': parser.parse('jcr:uuid'),
                'doc_name': remove_non_printable(os.path.basename("https://www.crucial.com" + parser.parse('jcr:path')) if (parser.parse('jcr:content.cq:name') is None or parser.parse('jcr:content.cq:name') =='None') else parser.parse('jcr:content.cq:name')).replace('_', '-').replace('/', '-').replace('\r\n', ''),
                'doc_url': "https://www.crucial.com" + parser.parse('jcr:path'),
                'last_modified_time': '1900-01-01 00:00:00' if (parser.parse('jcr:content.cq:lastReplicated') is None or parser.parse('jcr:content.cq:lastReplicated') == '') else datetime.strptime(parser.parse('jcr:content.cq:lastReplicated'), '%a %b %d %Y %H:%M:%S GMT%z').strftime('%Y-%m-%d %H:%M:%S'),
                'created_at': self.aem_instance.created_date,
                'updated_at': self.aem_instance.updated_date
            }
        return data
    def callback_trigger(self, operation):
        """
        Callback function to log Datastore ingestion results.

        This function is called by the Datastore client after each batch of documents is processed.
        It logs the results of the ingestion operation, including success and failure counts, to the SQL tables.

        Args:
            operation: The Datastore operation object.
        """
        global datastore_processed
        global data_extraction_ingestion_job_map
        out = datastore_callback(operation)
        batch_insert_status,datastore_failure_insert_status = log_datastore_results(out, data_extraction_ingestion_job_map, self)
        if batch_insert_status and datastore_failure_insert_status:
            datastore_processed+=1
        else:
            logger.error(f"Unable to update {self.genie_data_store_batch_history_table} table or {self.genie_error_log_table}")

        logger.info(f"Processed Requests: {datastore_processed}")
    def process_metadata(self,rows,aem_data):
        """
        Processes the metadata file to support the Vertex AI Search API
        """
        result_medata = []

        for row in rows:
            if row['doc_id'] in aem_data:
                data = aem_data[row['doc_id']]
                parser = SafeDictParser(data)
                tag_parser = SafeDictParser({k: (v if v is not None else '') for k, v in (x.split(':',1) if ':' in x else [x,None] for x in ast.literal_eval(parser.parse('jcr:content.metadata.cq:tags')) if ast.literal_eval(parser.parse('jcr:content.metadata.cq:tags')))})
                
                json_data = {
                    'doc_type' : 'aem-cpg',
                    'file_name' : remove_non_printable(os.path.basename("https://www.crucial.com" + parser.parse('jcr:path')) if (parser.parse('jcr:content.cq:name') is None or parser.parse('jcr:content.cq:name') =='None') else parser.parse('jcr:content.cq:name')).replace('_', '-').replace('/', '-').replace('\r\n', ''),
                    'file_name_keyprop': remove_non_printable(os.path.basename("https://www.crucial.com" + parser.parse('jcr:path')) if (parser.parse('jcr:content.cq:name') is None or parser.parse('jcr:content.cq:name') =='None') else parser.parse('jcr:content.cq:name')).replace('_', '-').replace('/', '-').replace('\r\n', ''),
                    'url' : remove_non_printable("https://www.crucial.com" + parser.parse('jcr:path')),
                    'pillar' : remove_non_printable('sales'),
                    'language' : remove_non_printable("{'English': 99.0}"),
                    
                    'deleted' : remove_non_printable('no'),
                    'author' : '' if (parser.parse('jcr:content.metadata.dc:creator') is None or parser.parse('jcr:content.metadata.dc:creator').lower()=="none") else remove_non_printable(parser.parse('jcr:content.metadata.dc:creator')),
                    'last_modified_time': remove_non_printable( '1900-01-01 00:00:00' if (parser.parse('jcr:content.jcr:lastModified') is None or parser.parse('jcr:content.jcr:lastModified') == '') else datetime.strptime(parser.parse('jcr:content.jcr:lastModified'), '%a %b %d %Y %H:%M:%S GMT%z').strftime('%Y-%m-%d %H:%M:%S')),
                    'title' : remove_non_printable(parser.parse('jcr:content.metadata.jcr:title')),
                    'file_type' : remove_non_printable(parser.parse('jcr:content.metadata.dc:format')),
                    'doc_id' : remove_non_printable(parser.parse('jcr:uuid')),
                    'aem_object_id': remove_non_printable(parser.parse('jcr:uuid')),
                    # 'publisher': remove_non_printable(parser.parse('jcr:createdBy')),
                    # 'last_replicated': remove_non_printable(parser.parse('jcr:content.cq:lastReplicated')),
                    # 'last_replicated_action': remove_non_printable(parser.parse('jcr:content.cq:lastReplicationAction')),
                    'aem_uuid': remove_non_printable(parser.parse('jcr:uuid')),
                    # 'published_content_last_modified_by': remove_non_printable(parser.parse('jcr:content.jcr:lastModifiedBy')),
                    # 'published_content_last_modified_time': remove_non_printable(parser.parse('jcr:content.jcr:lastModified')),
                    'doc_category': 'aem-cpg',
                    'last_modified_unix_time' : int(row['last_modified_unix_time']),
                    'last_modified_iso_time': datetime.fromtimestamp(int(row['last_modified_unix_time']), tz=timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ'),
                    'description': ' '.join([
                        simplify_text( remove_non_printable( parser.parse('jcr:content.metadata.dc:description') ).lower().replace(',', ' ').replace('-', ' ').replace('–', ' ').strip() ),
                    ]).strip(),
                    'subject': ' '.join([
                        parse_subject( remove_non_printable( parser.parse('jcr:content.metadata.dc:subject') ).lower() ),
                    ]).strip(),
                    'products': ' '.join( list( set(
                        simplify_text( remove_non_printable( tag_parser.parse('micron-products') ).lower().replace(',', ' ').replace('-', ' ').replace('–', ' ').strip() ).split() +\
                        simplify_text( remove_non_printable( tag_parser.parse('crucial-products') ).lower().replace(',', ' ').replace('-', ' ').replace('–', ' ').strip() ).split()
                    ) ) ).strip(),
                    'optional_text_field_1': ' '.join([
                        simplify_text( remove_non_printable( tag_parser.parse('application') ).lower().replace(',', ' ').replace('-', ' ').replace('–', ' ').strip() ),
                    ]).strip(),
                    'optional_text_field_2': ' '.join([
                        simplify_text( remove_non_printable( tag_parser.parse('asset-type') ).lower().replace(',', ' ').replace('-', ' ').replace('–', ' ').strip() ),
                    ]).strip(),
                    'optional_text_field_3': ' '.join([
                        simplify_text( remove_non_printable( tag_parser.parse('segment') ).lower().replace(',', ' ').replace('-', ' ').replace('–', ' ').strip() ),
                    ]).strip(),
                    'optional_text_field_4': ' '.join([
                        simplify_text( remove_non_printable( tag_parser.parse('technology') ).lower().replace(',', ' ').replace('-', ' ').replace('–', ' ').strip() ),
                        simplify_text( remove_non_printable( tag_parser.parse('product-type') ).lower().replace(',', ' ').replace('-', ' ').replace('–', ' ').strip() ),
                        simplify_text( remove_non_printable( tag_parser.parse('form-factor') ).lower().replace(',', ' ').replace('-', ' ').replace('–', ' ').strip() ),
                    ]).strip(),
                    'optional_text_field_5': ' '.join([
                        simplify_text( remove_non_printable( tag_parser.parse('brand') ).lower().replace(',', ' ').replace('-', ' ').replace('–', ' ').strip() ),
                    ]).strip(),

                    # NER metadata.
                    'business_unit_list': build_ner_data(
                        [
                            remove_non_printable(os.path.basename("https://www.crucial.com" + parser.parse('jcr:path')) if parser.parse('jcr:content.cq:name') is None else parser.parse('jcr:content.cq:name')),
                            remove_non_printable("https://www.crucial.com" + parser.parse('jcr:path')),
                            ' '.join([simplify_text( remove_non_printable( parser.parse('jcr:content.metadata.dc:description') ).lower().replace(',', ' ').replace('-', ' ').replace('–', ' ').strip() )]),
                            ' '.join([parse_subject( remove_non_printable( parser.parse('jcr:content.metadata.dc:subject') ).lower() )]),
                            ' '.join( list( set(
                                simplify_text( remove_non_printable( tag_parser.parse('micron-products') ).lower().replace(',', ' ').replace('-', ' ').replace('–', ' ').strip() ).split() +\
                                simplify_text( remove_non_printable( tag_parser.parse('crucial-products') ).lower().replace(',', ' ').replace('-', ' ').replace('–', ' ').strip() ).split()
                            ) ) ),
                            ' '.join([simplify_text( remove_non_printable( tag_parser.parse('application') ).lower().replace(',', ' ').replace('-', ' ').replace('–', ' ').strip() )]),
                            ' '.join([simplify_text( remove_non_printable( tag_parser.parse('asset-type') ).lower().replace(',', ' ').replace('-', ' ').replace('–', ' ').strip() )]),
                            ' '.join([simplify_text( remove_non_printable( tag_parser.parse('segment') ).lower().replace(',', ' ').replace('-', ' ').replace('–', ' ').strip() )]),
                            ' '.join([
                                simplify_text( remove_non_printable( tag_parser.parse('technology') ).lower().replace(',', ' ').replace('-', ' ').replace('–', ' ').strip() ),
                                simplify_text( remove_non_printable( tag_parser.parse('product-type') ).lower().replace(',', ' ').replace('-', ' ').replace('–', ' ').strip() ),
                                simplify_text( remove_non_printable( tag_parser.parse('form-factor') ).lower().replace(',', ' ').replace('-', ' ').replace('–', ' ').strip() ),
                            ]),
                            ' '.join([simplify_text( remove_non_printable( tag_parser.parse('brand') ).lower().replace(',', ' ').replace('-', ' ').replace('–', ' ').strip() )]),
                        ],
                        'bu',
                    ),
                    'market_subsegment_list': build_ner_data(
                        [
                            remove_non_printable(os.path.basename("https://www.crucial.com" + parser.parse('jcr:path')) if parser.parse('jcr:content.cq:name') is None else parser.parse('jcr:content.cq:name')),
                            remove_non_printable("https://www.crucial.com" + parser.parse('jcr:path')),
                            ' '.join([simplify_text( remove_non_printable( parser.parse('jcr:content.metadata.dc:description') ).lower().replace(',', ' ').replace('-', ' ').replace('–', ' ').strip() )]),
                            ' '.join([parse_subject( remove_non_printable( parser.parse('jcr:content.metadata.dc:subject') ).lower() )]),
                            ' '.join( list( set(
                                simplify_text( remove_non_printable( tag_parser.parse('micron-products') ).lower().replace(',', ' ').replace('-', ' ').replace('–', ' ').strip() ).split() +\
                                simplify_text( remove_non_printable( tag_parser.parse('crucial-products') ).lower().replace(',', ' ').replace('-', ' ').replace('–', ' ').strip() ).split()
                            ) ) ),
                            ' '.join([simplify_text( remove_non_printable( tag_parser.parse('application') ).lower().replace(',', ' ').replace('-', ' ').replace('–', ' ').strip() )]),
                            ' '.join([simplify_text( remove_non_printable( tag_parser.parse('asset-type') ).lower().replace(',', ' ').replace('-', ' ').replace('–', ' ').strip() )]),
                            ' '.join([simplify_text( remove_non_printable( tag_parser.parse('segment') ).lower().replace(',', ' ').replace('-', ' ').replace('–', ' ').strip() )]),
                            ' '.join([
                                simplify_text( remove_non_printable( tag_parser.parse('technology') ).lower().replace(',', ' ').replace('-', ' ').replace('–', ' ').strip() ),
                                simplify_text( remove_non_printable( tag_parser.parse('product-type') ).lower().replace(',', ' ').replace('-', ' ').replace('–', ' ').strip() ),
                                simplify_text( remove_non_printable( tag_parser.parse('form-factor') ).lower().replace(',', ' ').replace('-', ' ').replace('–', ' ').strip() ),
                            ]),
                            ' '.join([simplify_text( remove_non_printable( tag_parser.parse('brand') ).lower().replace(',', ' ').replace('-', ' ').replace('–', ' ').strip() )]),
                        ],
                        'market_subsegment',
                    ),
                    'mpn_list': build_ner_data(
                        [
                            remove_non_printable(os.path.basename("https://www.crucial.com" + parser.parse('jcr:path')) if parser.parse('jcr:content.cq:name') is None else parser.parse('jcr:content.cq:name')),
                            remove_non_printable("https://www.crucial.com" + parser.parse('jcr:path')),
                            ' '.join([simplify_text( remove_non_printable( parser.parse('jcr:content.metadata.dc:description') ).lower().replace(',', ' ').replace('-', ' ').replace('–', ' ').strip() )]),
                            ' '.join([parse_subject( remove_non_printable( parser.parse('jcr:content.metadata.dc:subject') ).lower() )]),
                            ' '.join( list( set(
                                simplify_text( remove_non_printable( tag_parser.parse('micron-products') ).lower().replace(',', ' ').replace('-', ' ').replace('–', ' ').strip() ).split() +\
                                simplify_text( remove_non_printable( tag_parser.parse('crucial-products') ).lower().replace(',', ' ').replace('-', ' ').replace('–', ' ').strip() ).split()
                            ) ) ),
                            ' '.join([simplify_text( remove_non_printable( tag_parser.parse('application') ).lower().replace(',', ' ').replace('-', ' ').replace('–', ' ').strip() )]),
                            ' '.join([simplify_text( remove_non_printable( tag_parser.parse('asset-type') ).lower().replace(',', ' ').replace('-', ' ').replace('–', ' ').strip() )]),
                            ' '.join([simplify_text( remove_non_printable( tag_parser.parse('segment') ).lower().replace(',', ' ').replace('-', ' ').replace('–', ' ').strip() )]),
                            ' '.join([
                                simplify_text( remove_non_printable( tag_parser.parse('technology') ).lower().replace(',', ' ').replace('-', ' ').replace('–', ' ').strip() ),
                                simplify_text( remove_non_printable( tag_parser.parse('product-type') ).lower().replace(',', ' ').replace('-', ' ').replace('–', ' ').strip() ),
                                simplify_text( remove_non_printable( tag_parser.parse('form-factor') ).lower().replace(',', ' ').replace('-', ' ').replace('–', ' ').strip() ),
                            ]),
                            ' '.join([simplify_text( remove_non_printable( tag_parser.parse('brand') ).lower().replace(',', ' ').replace('-', ' ').replace('–', ' ').strip() )]),
                        ],
                        'mpn',
                    ),
                    'design_id_list': build_ner_data(
                        [
                            remove_non_printable(os.path.basename("https://www.crucial.com" + parser.parse('jcr:path')) if parser.parse('jcr:content.cq:name') is None else parser.parse('jcr:content.cq:name')),
                            remove_non_printable("https://www.crucial.com" + parser.parse('jcr:path')),
                            ' '.join([simplify_text( remove_non_printable( parser.parse('jcr:content.metadata.dc:description') ).lower().replace(',', ' ').replace('-', ' ').replace('–', ' ').strip() )]),
                            ' '.join([parse_subject( remove_non_printable( parser.parse('jcr:content.metadata.dc:subject') ).lower() )]),
                            ' '.join( list( set(
                                simplify_text( remove_non_printable( tag_parser.parse('micron-products') ).lower().replace(',', ' ').replace('-', ' ').replace('–', ' ').strip() ).split() +\
                                simplify_text( remove_non_printable( tag_parser.parse('crucial-products') ).lower().replace(',', ' ').replace('-', ' ').replace('–', ' ').strip() ).split()
                            ) ) ),
                            ' '.join([simplify_text( remove_non_printable( tag_parser.parse('application') ).lower().replace(',', ' ').replace('-', ' ').replace('–', ' ').strip() )]),
                            ' '.join([simplify_text( remove_non_printable( tag_parser.parse('asset-type') ).lower().replace(',', ' ').replace('-', ' ').replace('–', ' ').strip() )]),
                            ' '.join([simplify_text( remove_non_printable( tag_parser.parse('segment') ).lower().replace(',', ' ').replace('-', ' ').replace('–', ' ').strip() )]),
                            ' '.join([
                                simplify_text( remove_non_printable( tag_parser.parse('technology') ).lower().replace(',', ' ').replace('-', ' ').replace('–', ' ').strip() ),
                                simplify_text( remove_non_printable( tag_parser.parse('product-type') ).lower().replace(',', ' ').replace('-', ' ').replace('–', ' ').strip() ),
                                simplify_text( remove_non_printable( tag_parser.parse('form-factor') ).lower().replace(',', ' ').replace('-', ' ').replace('–', ' ').strip() ),
                            ]),
                            ' '.join([simplify_text( remove_non_printable( tag_parser.parse('brand') ).lower().replace(',', ' ').replace('-', ' ').replace('–', ' ').strip() )]),
                        ],
                        'design_id',
                    ),
                    'functional_technology_list': build_ner_data(
                        [
                            remove_non_printable(os.path.basename("https://www.crucial.com" + parser.parse('jcr:path')) if parser.parse('jcr:content.cq:name') is None else parser.parse('jcr:content.cq:name')),
                            remove_non_printable("https://www.crucial.com" + parser.parse('jcr:path')),
                            ' '.join([simplify_text( remove_non_printable( parser.parse('jcr:content.metadata.dc:description') ).lower().replace(',', ' ').replace('-', ' ').replace('–', ' ').strip() )]),
                            ' '.join([parse_subject( remove_non_printable( parser.parse('jcr:content.metadata.dc:subject') ).lower() )]),
                            ' '.join( list( set(
                                simplify_text( remove_non_printable( tag_parser.parse('micron-products') ).lower().replace(',', ' ').replace('-', ' ').replace('–', ' ').strip() ).split() +\
                                simplify_text( remove_non_printable( tag_parser.parse('crucial-products') ).lower().replace(',', ' ').replace('-', ' ').replace('–', ' ').strip() ).split()
                            ) ) ),
                            ' '.join([simplify_text( remove_non_printable( tag_parser.parse('application') ).lower().replace(',', ' ').replace('-', ' ').replace('–', ' ').strip() )]),
                            ' '.join([simplify_text( remove_non_printable( tag_parser.parse('asset-type') ).lower().replace(',', ' ').replace('-', ' ').replace('–', ' ').strip() )]),
                            ' '.join([simplify_text( remove_non_printable( tag_parser.parse('segment') ).lower().replace(',', ' ').replace('-', ' ').replace('–', ' ').strip() )]),
                            ' '.join([
                                simplify_text( remove_non_printable( tag_parser.parse('technology') ).lower().replace(',', ' ').replace('-', ' ').replace('–', ' ').strip() ),
                                simplify_text( remove_non_printable( tag_parser.parse('product-type') ).lower().replace(',', ' ').replace('-', ' ').replace('–', ' ').strip() ),
                                simplify_text( remove_non_printable( tag_parser.parse('form-factor') ).lower().replace(',', ' ').replace('-', ' ').replace('–', ' ').strip() ),
                            ]),
                            ' '.join([simplify_text( remove_non_printable( tag_parser.parse('brand') ).lower().replace(',', ' ').replace('-', ' ').replace('–', ' ').strip() )]),
                        ],
                        'functional_technology',
                    ),
                    'package_type_list': build_ner_data(
                        [
                            remove_non_printable(os.path.basename("https://www.crucial.com" + parser.parse('jcr:path')) if parser.parse('jcr:content.cq:name') is None else parser.parse('jcr:content.cq:name')),
                            remove_non_printable("https://www.crucial.com" + parser.parse('jcr:path')),
                            ' '.join([simplify_text( remove_non_printable( parser.parse('jcr:content.metadata.dc:description') ).lower().replace(',', ' ').replace('-', ' ').replace('–', ' ').strip() )]),
                            ' '.join([parse_subject( remove_non_printable( parser.parse('jcr:content.metadata.dc:subject') ).lower() )]),
                            ' '.join( list( set(
                                simplify_text( remove_non_printable( tag_parser.parse('micron-products') ).lower().replace(',', ' ').replace('-', ' ').replace('–', ' ').strip() ).split() +\
                                simplify_text( remove_non_printable( tag_parser.parse('crucial-products') ).lower().replace(',', ' ').replace('-', ' ').replace('–', ' ').strip() ).split()
                            ) ) ),
                            ' '.join([simplify_text( remove_non_printable( tag_parser.parse('application') ).lower().replace(',', ' ').replace('-', ' ').replace('–', ' ').strip() )]),
                            ' '.join([simplify_text( remove_non_printable( tag_parser.parse('asset-type') ).lower().replace(',', ' ').replace('-', ' ').replace('–', ' ').strip() )]),
                            ' '.join([simplify_text( remove_non_printable( tag_parser.parse('segment') ).lower().replace(',', ' ').replace('-', ' ').replace('–', ' ').strip() )]),
                            ' '.join([
                                simplify_text( remove_non_printable( tag_parser.parse('technology') ).lower().replace(',', ' ').replace('-', ' ').replace('–', ' ').strip() ),
                                simplify_text( remove_non_printable( tag_parser.parse('product-type') ).lower().replace(',', ' ').replace('-', ' ').replace('–', ' ').strip() ),
                                simplify_text( remove_non_printable( tag_parser.parse('form-factor') ).lower().replace(',', ' ').replace('-', ' ').replace('–', ' ').strip() ),
                            ]),
                            ' '.join([simplify_text( remove_non_printable( tag_parser.parse('brand') ).lower().replace(',', ' ').replace('-', ' ').replace('–', ' ').strip() )]),
                        ],
                        'package_type',
                    ),
                    'density_list': build_ner_data(
                        [
                            remove_non_printable(os.path.basename("https://www.crucial.com" + parser.parse('jcr:path')) if parser.parse('jcr:content.cq:name') is None else parser.parse('jcr:content.cq:name')),
                            remove_non_printable("https://www.crucial.com" + parser.parse('jcr:path')),
                            ' '.join([simplify_text( remove_non_printable( parser.parse('jcr:content.metadata.dc:description') ).lower().replace(',', ' ').replace('-', ' ').replace('–', ' ').strip() )]),
                            ' '.join([parse_subject( remove_non_printable( parser.parse('jcr:content.metadata.dc:subject') ).lower() )]),
                            ' '.join( list( set(
                                simplify_text( remove_non_printable( tag_parser.parse('micron-products') ).lower().replace(',', ' ').replace('-', ' ').replace('–', ' ').strip() ).split() +\
                                simplify_text( remove_non_printable( tag_parser.parse('crucial-products') ).lower().replace(',', ' ').replace('-', ' ').replace('–', ' ').strip() ).split()
                            ) ) ),
                            ' '.join([simplify_text( remove_non_printable( tag_parser.parse('application') ).lower().replace(',', ' ').replace('-', ' ').replace('–', ' ').strip() )]),
                            ' '.join([simplify_text( remove_non_printable( tag_parser.parse('asset-type') ).lower().replace(',', ' ').replace('-', ' ').replace('–', ' ').strip() )]),
                            ' '.join([simplify_text( remove_non_printable( tag_parser.parse('segment') ).lower().replace(',', ' ').replace('-', ' ').replace('–', ' ').strip() )]),
                            ' '.join([
                                simplify_text( remove_non_printable( tag_parser.parse('technology') ).lower().replace(',', ' ').replace('-', ' ').replace('–', ' ').strip() ),
                                simplify_text( remove_non_printable( tag_parser.parse('product-type') ).lower().replace(',', ' ').replace('-', ' ').replace('–', ' ').strip() ),
                                simplify_text( remove_non_printable( tag_parser.parse('form-factor') ).lower().replace(',', ' ').replace('-', ' ').replace('–', ' ').strip() ),
                            ]),
                            ' '.join([simplify_text( remove_non_printable( tag_parser.parse('brand') ).lower().replace(',', ' ').replace('-', ' ').replace('–', ' ').strip() )]),
                        ],
                                            'density',
                    ),
                    'product_series_list': build_ner_data(
                        [
                            remove_non_printable(os.path.basename("https://www.crucial.com" + parser.parse('jcr:path')) if parser.parse('jcr:content.cq:name') is None else parser.parse('jcr:content.cq:name')),
                            remove_non_printable("https://www.crucial.com" + parser.parse('jcr:path')),
                            ' '.join([simplify_text( remove_non_printable( parser.parse('jcr:content.metadata.dc:description') ).lower().replace(',', ' ').replace('-', ' ').replace('–', ' ').strip() )]),
                            ' '.join([parse_subject( remove_non_printable( parser.parse('jcr:content.metadata.dc:subject') ).lower() )]),
                            ' '.join( list( set(
                                simplify_text( remove_non_printable( tag_parser.parse('micron-products') ).lower().replace(',', ' ').replace('-', ' ').replace('–', ' ').strip() ).split() +\
                                simplify_text( remove_non_printable( tag_parser.parse('crucial-products') ).lower().replace(',', ' ').replace('-', ' ').replace('–', ' ').strip() ).split()
                            ) ) ),
                            ' '.join([simplify_text( remove_non_printable( tag_parser.parse('application') ).lower().replace(',', ' ').replace('-', ' ').replace('–', ' ').strip() )]),
                            ' '.join([simplify_text( remove_non_printable( tag_parser.parse('asset-type') ).lower().replace(',', ' ').replace('-', ' ').replace('–', ' ').strip() )]),
                            ' '.join([simplify_text( remove_non_printable( tag_parser.parse('segment') ).lower().replace(',', ' ').replace('-', ' ').replace('–', ' ').strip() )]),
                            ' '.join([
                                simplify_text( remove_non_printable( tag_parser.parse('technology') ).lower().replace(',', ' ').replace('-', ' ').replace('–', ' ').strip() ),
                                simplify_text( remove_non_printable( tag_parser.parse('product-type') ).lower().replace(',', ' ').replace('-', ' ').replace('–', ' ').strip() ),
                                simplify_text( remove_non_printable( tag_parser.parse('form-factor') ).lower().replace(',', ' ').replace('-', ' ').replace('–', ' ').strip() ),
                            ]),
                            ' '.join([simplify_text( remove_non_printable( tag_parser.parse('brand') ).lower().replace(',', ' ').replace('-', ' ').replace('–', ' ').strip() )]),
                        ],
                        'product_series',
                    ),
                    'process_requirement_list': build_ner_data(
                        [
                            remove_non_printable(os.path.basename("https://www.crucial.com" + parser.parse('jcr:path')) if parser.parse('jcr:content.cq:name') is None else parser.parse('jcr:content.cq:name')),
                            remove_non_printable("https://www.crucial.com" + parser.parse('jcr:path')),
                            ' '.join([simplify_text( remove_non_printable( parser.parse('jcr:content.metadata.dc:description') ).lower().replace(',', ' ').replace('-', ' ').replace('–', ' ').strip() )]),
                            ' '.join([parse_subject( remove_non_printable( parser.parse('jcr:content.metadata.dc:subject') ).lower() )]),
                            ' '.join( list( set(
                                simplify_text( remove_non_printable( tag_parser.parse('micron-products') ).lower().replace(',', ' ').replace('-', ' ').replace('–', ' ').strip() ).split() +\
                                simplify_text( remove_non_printable( tag_parser.parse('crucial-products') ).lower().replace(',', ' ').replace('-', ' ').replace('–', ' ').strip() ).split()
                            ) ) ),
                            ' '.join([simplify_text( remove_non_printable( tag_parser.parse('application') ).lower().replace(',', ' ').replace('-', ' ').replace('–', ' ').strip() )]),
                            ' '.join([simplify_text( remove_non_printable( tag_parser.parse('asset-type') ).lower().replace(',', ' ').replace('-', ' ').replace('–', ' ').strip() )]),
                            ' '.join([simplify_text( remove_non_printable( tag_parser.parse('segment') ).lower().replace(',', ' ').replace('-', ' ').replace('–', ' ').strip() )]),
                            ' '.join([
                                simplify_text( remove_non_printable( tag_parser.parse('technology') ).lower().replace(',', ' ').replace('-', ' ').replace('–', ' ').strip() ),
                                simplify_text( remove_non_printable( tag_parser.parse('product-type') ).lower().replace(',', ' ').replace('-', ' ').replace('–', ' ').strip() ),
                                simplify_text( remove_non_printable( tag_parser.parse('form-factor') ).lower().replace(',', ' ').replace('-', ' ').replace('–', ' ').strip() ),
                            ]),
                            ' '.join([simplify_text( remove_non_printable( tag_parser.parse('brand') ).lower().replace(',', ' ').replace('-', ' ').replace('–', ' ').strip() )]),
                        ],
                        'process_requirement',
                    )
                }
                
                
                json_data['title']= json_data['title'].replace("_","-").replace("/","-").replace("\r\n","")
                json_data['file_name']= json_data['file_name'].replace("_","-").replace("/","-").replace("\r\n","")
                json_data['file_name_keyprop']= json_data['file_name_keyprop'].replace("_","-").replace("/","-").replace("\r\n","")
                m_dict = {
                    "id": "doc-" + str(row['profile_id']),
                    "jsonData": json_data,
                    "content": {
                        "mimeType": "application/pdf" if row['file_type'].lower() == 'pdf' else 
                        "application/vnd.openxmlformats-officedocument.presentationml.presentation" if (row['file_type'].lower() == 'pptx' or row['file_type'].lower() == 'ppt') else
                        "application/vnd.openxmlformats-officedocument.wordprocessingml.document" if (row['file_type'].lower() == 'docx' or row['file_type'].lower() == 'doc') else
                        "text/html" if (row['file_type'].lower() =='htm' or row['file_type'].lower() == 'html') else
                        row['file_type'],
                        "uri": "gs://" + self.aem_instance.private_bucket_name + '/' + self.aem_instance.document_folder + self.aem_instance.date + '/' + self.aem_instance.encode_file_name(row['file_name'],row['profile_id'])  
                    }
                }
                
                
                
               
                if m_dict['jsonData']['title'].lower() == "none" :
                   m_dict['jsonData']['title'] = m_dict['jsonData']['file_name']
                
                
                m_dict['jsonData']['last_modified_unix_time'] = int(float(m_dict['jsonData']['last_modified_unix_time']))
                m_dict['jsonData'] = json.dumps(m_dict['jsonData'])
                json.dumps(result_medata.append(m_dict),ensure_ascii=False)
        return result_medata

def main():
    """
    The main entry point of the script
    Execution flow:
        1. Extract Delta Metadata from the AEM API. (2021-01-01 is the start date considered for Intial/History load)
        2. Upsert the metadata into Alloydb tables.
        3. Fetch the tables for document relative path's that are waiting for download.
        4. segregate the data into predefined size of batch (eg: if 200 MB is predefined size then each batches are divided such that each batch has collective file size of 200 MB appox.)
        5. Download the files and upload them into GCS
        6. Upload the corresponding batch metadata and batch file.
        7. Trigger ML Pipeline through pubsub message
    """
    try:
        datastore_triggered = 0
        global data_extraction_ingestion_job_map
        
        logger.info("=====================================")
        logger.info("JOB STARTED")
        
        current_dir = os.path.dirname(os.path.abspath(__file__))
        config_path = GOOGLE_CLOUD_PROJECT + "_aem_" + "config.yaml"
        config_path = os.path.join(current_dir,"config", config_path)

        
        
        aem_instance = AEM(config_path)
        gcp_utils_instance = GcpUtils(aem_instance)
        aem_instance.extract_db_creds(gcp_utils_instance)
        aem_instance.extract_etl_creds(gcp_utils_instance)
        aem_instance.access_token = aem_instance.get_token()
        datastore = Datastore(datastore_id= aem_instance.datastore_id)

        #query sites from table
        aem_paths = gcp_utils_instance.execute_sql(aem_instance.all_sites_query,'SELECT').values.tolist()

        for aem_path in aem_paths:
            run_id = uuid.uuid4()
            aem_location = aem_path[0]
            source_type = aem_path[1]
            aem_type = aem_path[2]
            gcs_metadata_path = []
            
           
            logger.info("aem_location: %s, source_type: %s, aem_type: %s", aem_location, source_type, aem_type)
            
            try:
                #aem_instance.define_aem_cpg_api_filter()
                aem_instance.define_delta_date_query(aem_location)
                logger.info("JOB STARTED FOR AEM LOCATION: %s",aem_location)

                #extract delta timestamp from previous run
                res = gcp_utils_instance.execute_sql(aem_instance.delta_date_query,'SELECT')
                logger.info("Latest Timestamp to start extracting metdata for location %s : %s", aem_location, res.last_time_flag[0])
                
                doc_info = {'doc_info_metadata':[]}
                search_profile = {'search_profile_metadata':[]}
                
                if source_type == 'aem-dam':
                    aem_type_instance = AEM_DAM(aem_instance,gcp_utils_instance)
                elif source_type == 'aem-cpg':
                    aem_type_instance = AEM_CPG(aem_instance,gcp_utils_instance)

                aem_data = aem_type_instance.generate_metadata_file(doc_info,search_profile, aem_location, res.last_time_flag[0])
                logger.info("Number of New Documents Found in location %s : %s", aem_location, len(doc_info['doc_info_metadata']))
                
                # if we have new records in sharepoint then insert the data into metadata tables for extraction.
                if len(doc_info['doc_info_metadata']) > 0:
                    #insert into doc_info table
                    doc_info_batches = [doc_info['doc_info_metadata'][i:i+aem_instance.postgres_insert_batch_size] for i in range(0,len(doc_info['doc_info_metadata']),aem_instance.postgres_insert_batch_size)]
                    for doc_info_batch in doc_info_batches:
                        doc_info_merge_query = aem_instance.generate_bulk_insert_with_merge_query(aem_instance.doc_info_table, pd.DataFrame(doc_info_batch).drop_duplicates().to_dict('records'), aem_instance.unique_key)
                        gcp_utils_instance.execute_sql(doc_info_merge_query,'INSERT')
                    logger.info("For location %s inserted data into %s table ", aem_location, aem_instance.doc_info_table)

                    #insert into search_profile table
                    search_profile_batches = [search_profile['search_profile_metadata'][i:i+aem_instance.postgres_insert_batch_size] for i in range(0,len(search_profile['search_profile_metadata']),aem_instance.postgres_insert_batch_size)]
                    for search_profile_batch in search_profile_batches:
                        search_profile_merge_query = aem_instance.generate_bulk_insert_with_merge_query(aem_instance.search_profile_table, pd.DataFrame(search_profile_batch).drop_duplicates().to_dict('records'), aem_instance.unique_key)
                        gcp_utils_instance.execute_sql(search_profile_merge_query,'INSERT')
                    logger.info("For location %s Inserted data into %s table", aem_location, aem_instance.search_profile_table)

                    #insert into job_history table
                    aem_instance.define_history_insert_query(aem_location)
                    result = gcp_utils_instance.execute_sql(aem_instance.job_insert,'SELECT').to_dict(orient='records')
                    history_log = []
                    for data in result:
                        history_log.append(aem_instance.parse_log_result_job_history(data,'0','Ready for Download',str(run_id)))
                    history_download_batches = [history_log[i:i+aem_instance.postgres_insert_batch_size] for i in range(0,len(history_log),aem_instance.postgres_insert_batch_size)]
                    for history_download_batch in history_download_batches:
                        history_download_log_query = aem_instance.generate_bulk_insert_query(aem_instance.job_history_table,history_download_batch)
                        gcp_utils_instance.execute_sql(history_download_log_query,'INSERT')
                    logger.info("For location %s inserted waiting for download data into %s table", aem_location, aem_instance.job_history_table)
                    
                #fetch records from metadata for document extraction and upload
                aem_instance.define_metadata_query(aem_location)
                result = gcp_utils_instance.execute_sql(aem_instance.metadata_query,'SELECT').to_dict(orient='records')
                logger.info("For location %s Number of files waiting for download: %s", aem_location, len(result))

                # if there are new records to be downloaded
                if len(result) > 0:
                    #data upload - zip, encypted
                    job_id = str(run_id) +'.'+ result[0]['doc_type']
                    job_history = {'job_history_log':[]}
                    
                    #download the files of a batch & upload the Files
                    try:
                        history = aem_instance.download_batch_files(result, str(run_id), aem_type_instance, gcp_utils_instance)
                    except Exception as download_exception:
                        logger.error("Failed while downloading the file")
                        logger.error(download_exception, exc_info=True)
                    logger.info("For location %s download is completed", aem_location)
                    
                    
                    #Get the download failed records
                    failure_records = [h_data['profile_id'] for h_data in history if h_data.get('status_code') == '2']
                    
                    # upload the metadata to the batch
                    m_data = copy.deepcopy(result)
                    filered_m_data = [fm_data for fm_data in m_data if fm_data.get('profile_id') not in failure_records]
                    
                    metadata_filename = gcp_utils_instance.upload_metadata_gcs('\n'.join(json.dumps(obj,ensure_ascii=False) for obj in aem_type_instance.process_metadata(filered_m_data,aem_data)),job_id)
                    gcs_metadata_path.append('gs://' + aem_instance.bucket_name + '/' + metadata_filename)
                    logger.info("For location %s uploading metadata is completed, Filename: %s", aem_location, metadata_filename)

                    #insert the Download success message to job_history table
                    job_history['job_history_log'].append(history)
                    history_log_query = aem_instance.generate_bulk_insert_query(aem_instance.job_history_table, job_history['job_history_log'][0])

                    gcp_utils_instance.execute_sql(history_log_query,'INSERT')
                    logger.info("For location %s Inserted success data into %s table", aem_location, aem_instance.job_history_table)
                        
                    #Trigger ML pipeline
                    if gcs_metadata_path:
                        datastore_triggered += 1
                        datastore.import_documents(gcs_uris=gcs_metadata_path, call_back_function=aem_instance.callback_trigger)  # trigger Datastore Ingestion

                        # map created to link the Data extraction and Ingestion job_ids and form a lineage for records
                        datastore_return_id = datastore.import_operations[-1]._operation.name.split('/')[-1]
                        data_extraction_ingestion_job_map.update({datastore_return_id: job_id})
                    else:
                        logger.info(f"No records pending for download for site: {aem_location}")
            except Exception as overall_ex:
                logger.error("*********************SOME FAILURE OCCURRED, REFER BELOW*************************")
                logger.error(overall_ex, exc_info=True)
                issue_update = "INSERT INTO %s (created_date, site, url, error_log) values ('%s','%s','','%s')" % (aem_instance.failure_log_table,aem_instance.created_date,aem_location,str(type(overall_ex).__name__)+"-" +str(type(overall_ex).__doc__))
                gcp_utils_instance.execute_sql(issue_update,'INSERT')
                continue
        if datastore_triggered > 0:
            if datastore.wait_for_pending_operations(max_wait=600) and datastore_triggered == datastore_processed:   #wait for Datastore Ingestion to complete
                logger.info(f'datastore_triggered: {datastore_triggered}, datastore_processed: {datastore_processed}')
                logger.info('All datastore operations including SQL logging is completed!')
            else:
                logger.info(f'datastore_triggered: {datastore_triggered}, datastore_processed: {datastore_processed}')
                logger.critical("Datastore Processing has not completed within 10min or the triggered and processed entries are not matching, please evaluate")
        else:
            logger.info("No New Records Extracted")
        logger.info("JOB COMPLETED")
        logger.info("=====================================")
    except Exception as overall_ex:
        logger.error("*********************SOME FAILURE OCCURRED, REFER BELOW*************************")
        logger.error(overall_ex, exc_info=True)

if __name__ == "__main__":
    main()
