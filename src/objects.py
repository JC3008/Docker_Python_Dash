# importing libraries
import sys 
sys.path.append(r'C:\Users\SALA443\Desktop\Projetos\DadosB3_Teste2\venv\Lib\site-packages')
import datetime as dt
from datetime import datetime,date,time
import zipfile
import requests
from bs4 import BeautifulSoup
import re
import os
import logging
import boto3
import pandas as pd
from botocore.exceptions import ClientError
from pathlib import Path
from dotenv import load_dotenv
import io
import uuid
import yaml
import requests
from bs4 import *
from urllib.request import Request, urlopen
import zipfile
from tempfile import TemporaryDirectory
from pathlib import Path
import os

# Setting up variables

process_id = uuid.uuid4().hex[:16]
s3_sufix = '727477891012'
s3_prefix = 'de'
layers = {'landing':'landing','processed':'processed','curated':'curated','consume':'consume'}
enviroment = {'prod':'prod','dev':'dev','default':'default'}
project = {'default':'default', 'okonomikus':'okonomikus','test':'test'}
provider = {'s3':'s3','local':'local','default':'default'}
profile = {'admin':'admin', 'data_engineer':'data_engineer','tester':'tester'}
source = layers
target = layers

logging.basicConfig(
    
        level=logging.INFO,
        handlers=[logging.FileHandler("dadoseconomicos.log", mode='w'),
                  logging.StreamHandler()],
        format="%(message)s -  %(funcName)s - %(filename)s - %(asctime)s"
        )

# dotenv_path = Path(r'C:\Users\SALA443\Desktop\Projetos\DadosB3_Teste2\Python\src\.env')
# load_dotenv(dotenv_path=dotenv_path)

dotenv_path = Path(r'/workspaces/app/.env')
load_dotenv(dotenv_path=dotenv_path)

year = dt.date.today().year
month = dt.date.today().month
day = dt.date.today().day
hour = datetime.now().strftime("%H")
current_time = datetime.now().strftime("%H:%M:%S")
sep = {'standard':';','altered':','}
encoding = {'standard':'utf-8','altered':'Windows-1252'}
tags = {'ppl_fundamentus':'S3|Fundamentus|B3',
        'ppl_dfp':'S3|DFP|B3|CVM_OpenData',
        'ppl_cad':'S3|DFP|B3|CVM_OpenData'}

class s3_bucket_methods():
    '''
    This class was minded for handy s3 buckets operations like retriving file
    list and others.
    '''
    def listing_files_from_a_bucket(bucket,prefix,profile,provider):
        '''
        Function developed for iterate over a given s3 bucket/key and stores
        each file name into a list.
        
        Returns a list.
        
        By passing bucket name and prefix, it is possible to retrieve the file names.
        
        I.E
        listing_files_from_a_bucket(
            'de-okkus-landing-dev-727477891012',
            '2023/12/31/fundamentus')
        '''
        
        credentials = aws_connection(profile=profile,provider=provider).account

        client = boto3.client('s3',                              
                                aws_access_key_id     =   credentials['aws_access_key_id'],
                                aws_secret_access_key =   credentials['aws_secret_access_key']
                        )

        s3_response = client.list_objects_v2(
            Bucket=bucket,
            Prefix=prefix
        )
        
        s3_file_list = []
        for x in s3_response['Contents']: 
            x = str(x['Key']).split('/')
            s3_file_list.append(x[-1])   

        return s3_file_list


class scheduled():
    '''
    This class performs an inteligent way to choose between automatically running
    pipelines and manually triggerd pipelines. This approach provides the opportunity 
    to input sensitive information such as keys and passwords without saving it.
    1 is used for cases when the pipelines are scheduled
    0 is for manually triggered ones    
    '''
    def __init__(self,scheduled:bool):
        self.scheduled = scheduled   
    
    def on_off(self):  
        if self.scheduled == 0:
            return 0
        elif self.scheduled == 1:
            return 1 
        else:
            logging.error('Please insert a valid number! (1 for scheduled pipelines or 0 for manually triggered pipelines)')     
        
class frequency():
    
    '''
    This class aims to build folder structure in a standard form, like described bellow:
    
    daily = yyyy/mm/dd/
    monthly = yyyy/mm/
    hourly = yyyy/mm/dd/hh
    
    As a result is expected the avoidance of issues by mistypped values and agility on the folder building process.     
    '''
    
    def __init__(self,frequency=None):   
        pipeline_method = scheduled(1).on_off()
        
        if pipeline_method == 0: 
            self.frequency = input("Enter a frequency!") 
        else:
            self.frequency = frequency
    
    def define(self):    
            '''
            The *Define method is for effectively choose a type of folder structure.
            It depends on the variable {pipeline_method} defined in the __init__ method.
            When {pipeline_method} is equal 1 the function uses the value passed inside curly brackets, and when it is equal 0 the function uses the value typed manually.
            '''  
                
            if  self.frequency == 'daily':
                return f"{year}/{str(month).zfill(2)}/{str(day).zfill(2)}/"
            elif self.frequency == 'monthly':
                return f"{year}/{str(month).zfill(2)}/"
            elif self.frequency == 'hourly':
                return f"{year}/{str(month).zfill(2)}/{str(day).zfill(2)}/{str(hour).zfill(2)}/"
            else:
                logging.error("Enter a valid frequency! [daily, monthly, hourly] is allowed.")
                raise TypeError("A invalid value was typed for frequency. Please check if it was mistyped or it is a new frequency that has to be included in valid frequency list")
           
# freq = frequency('daily').define()

class aws_connection():
    '''
    This class performs the aws connection, by fetching 
    credentials within .env file.
    
    account method: Fetches access_key_id and access_secret_id in .env and 
    returns a dictionary containning sensitive information.
    
    conn method: Establish the connection and returns client object.
    
    aws_connection('admin','s3').conn()
    '''
    def __init__(self,profile:str,provider:str,aws_access_key=None,aws_access_secret_key=None):
        
        self.profile    = profile   
        self.provider   = provider   
          
        pipeline_method = scheduled(1).on_off()  
        
        log_dict = {
            1:"The pipeline method was set as automatic.",
            0:"The pipeline method was set as manual, which means some of parameters will be required manually"}   
        
        # log checkpoint
        logging.info(log_dict[pipeline_method])   
        
        if pipeline_method == 0: 
            self.aws_access_key = input("Please enter your access key id!") 
            self.aws_access_secret_key = input("Please type your secret key!") 
        else:
            self.aws_access_key = aws_access_key
            self.aws_access_secret_key = aws_access_secret_key 
             
                   
        
        
    @property
    def account(self) -> dict:
        
        if self.profile == "admin":
            
            # log checkpoint
            logging.info('admin profile was chosen')
            return {"aws_access_key_id":os.getenv("aws_access_key_id"),
                    "aws_secret_access_key":os.getenv("aws_secret_access_key"),
                    "aws_region":os.getenv("aws_region")}
            
        else:
            # log checkpoint
            logging.info("Invalid profile or .env file secrets!")
            return {"aws_access_key_id":os.getenv("excepetion_aws_access_key_id"),
                    "aws_secret_access_key":os.getenv("excepetion_aws_secret_access_key"),
                    "aws_region":os.getenv("excepetion_aws_region")}
    
    def conn(self):
        
        try:            
            credentials = aws_connection(self.profile,self.provider).account            
            client = boto3.client(self.provider,                              
                        aws_access_key_id     =   credentials['aws_access_key_id'],
                        aws_secret_access_key =   credentials['aws_secret_access_key']
                )
            logging.info(f"Access granted for credentials starting with {credentials['aws_access_key_id'][:7]}")
            
            return client
            
        
        except ValueError(f"Access denied! Please check if the aws_access_key_id and aws_secret_access_key are correctly typed in .env for {self.profile} profile."):
            logging.error(f"Access denied for {self.profile}")
        
class path_name():
    '''
    This class is for standardize the folder structure for datalakes and 
    local projects.
    
    Parameters: \n
    self.enviroment (str, required) \n
    self.provider (str, required) \n
    self.profile (str, required) \n
    self.layer (str, required) \n
    self.project (str, required) \n 
    self.key (str, required) \n   
    self.context (str, None) 
    
    Methods:
    define()
    
    '''
    def __init__(self,enviroment:str,project:str,layer:str,key:str,
                 provider:str,profile:str,source=None,target=None,file_name=None,context=None
                 ):
        
        self.enviroment     = enviroment
        self.provider       = provider
        self.profile        = profile
        self.layer          = layer
        self.project        = project  
        self.key            = key 
        self.source         = source
        self.target         = target        
        self.file_name      = file_name
        self.context        = context
        
        
    def check_index(self):
        '''This method performs validation for inputed index range for
        all parameters'''
        # index = [self.enviroment, self.provider, self.profile, self.layer, self.project]
                 
        if self.enviroment not in enviroment.values() or self.provider not in provider.values() or self.profile not in profile.values() or self.layer not in layers.values() or self.source not in source.values() or self.target not in target.values() or self.project not in project.values():
            logging.error("Unexpected value passed as parameter.\nCheck the docstring for path_name().define.")
            raise Exception(f"Check if the typed parameters:\n [layers, enviroment, project, provider, profile, source, target] matched with the variables defined in the beginning of the objects.py file") 
            
        else:
            logging.info(f"All parameters was validated!")      
       
    def define(self):
        
        '''
        Use the dictionaries bellow to properly map your inputs: \n
        It is recommended to use *frequency().define() in place of key parameter.\n
        
        s3_sufix = '727477891012'
        s3_prefix = 'de'
        layers = {'landing':'landing','processed':'processed','curated':'curated','consume':'consume'}
        enviroment = {'prod':'prod','dev':'dev','default':'default'}
        project = {'default':'default', 'okonomikus':'okonomikus','test':'test'}
        provider = {'s3':'s3','local':'local','default':'default'}
        profile = {'admin':'admin', 'data_engineer':'data_engineer','tester':'tester'}
        source = layers
        target = layers\n
        
        examples:\n
        
        '''                
        path_name.check_index(self)
        
        process_id = uuid.uuid4().hex[:16]
        s3_sufix = '727477891012'
        s3_prefix = 'de'
        layers = {'landing':'landing','processed':'processed','curated':'curated','consume':'consume'}
        enviroment = {'prod':'prod','dev':'dev','default':'default'}
        project = {'default':'default', 'okonomikus':'okkus','test':'test'}
        provider = {'s3':'s3','local':'local','default':'default'}
        profile = {'admin':'admin', 'data_engineer':'data_engineer','tester':'tester'}
        str_path = f"{enviroment[self.enviroment]}/{project[self.project]}/{layers[self.layer]}/{self.key}"
        s3_str_path = f"{enviroment[self.enviroment]}-{project[self.project]}-{layers[self.layer]}/"
        key = f'{self.key}{self.context}/'
        source = {'landing':'landing','processed':'processed','curated':'curated','consume':'consume'}
        s3_source = f"{s3_prefix}-{project[self.project]}-{layers[self.source]}-{enviroment[self.enviroment]}-{s3_sufix}"
        target = {'landing':'landing','processed':'processed','curated':'curated','consume':'consume'}
        s3_target = f"{s3_prefix}-{project[self.project]}-{layers[self.target]}-{enviroment[self.enviroment]}-{s3_sufix}"

        
        result = {
            'path':str_path,
            's3_path':s3_str_path,
            'key':key,
            'enviroment':enviroment[self.enviroment],
            'project':project[self.project],
            'layer':layers[self.layer],
            'provider':provider[self.provider],
            'profile':profile[self.profile],
            'source':source[self.source],
            's3_source':s3_source,
            'target':target[self.target],
            's3_target':s3_target,
            's3_sufix':s3_sufix,
            's3_prefix':s3_prefix,
            'filename':self.file_name
                        
        }
        
        logging.info(f"{self.file_name} | {profile[self.profile]} profile is in charge of building {provider[self.provider]} storage under the process_id {process_id}.")
        # return f"{enviroment[self.enviroment]}/{project[self.project]}/{layers[self.layer]}/{self.key}"
        return result
    
class data_transfer():
    '''
    This class performs data transfer between S3 buckets and add metadata fields in
    order to better management of the dataflow. The goal of that feature is to provide 
    agility whenever debugging is neeeded.
    '''
    bucket = dict
    identity = str
    def __init__(self,
                 source:str,
                 target:str,
                 provider:str,
                 profile:str,
                 file:object,
                 pipeline=str,
                 pipeline_id=None,
                 tag=None,
                 key=None,
                 filename=None):
        
        self.source         = source
        self.target         = target
        self.provider       = provider
        self.profile        = profile
        self.file           = file
        self.pipeline       = pipeline
        self.pipeline_id    = pipeline_id
        self.tag            = tag
        self.key            = key
        self.filename       = filename
   
    def transfer(self):
        '''
        transfer method aims to move data from one S3 bucket to another, adding 
        new fields.
        4 fields are added by default:
            identity: Field which contains the pipeline name and pipeline_id
            loaded_{*Target bucket name}_date: Date of data uploading
            loaded_{*Target bucket name}_time: Time of data uploading
            tags: Tags that are binded to pipeline name
            
        To perform the transfer method, write as bellow:
        
        data_transfer(source='landing',
                target='processed',
                provider='s3',
                profile='admin',
                file=pd.DataFrame(),
                pipeline='ppl_fundamentus').transfer()
                
        This function will get fundametus.csv file from S3 source to target and also will
        add the four metadata fields mentioned before.
        
        '''        
        client = aws_connection('admin','s3').conn()
        # credentials = aws_connection(profile=self.profile).account
        
        # client = boto3.client(self.provider,                              
        #                       aws_access_key_id     =   credentials['aws_access_key_id'],
        #                       aws_secret_access_key =   credentials['aws_secret_access_key']
        #                 )
        
        logging.info(f"AWS connection was establshed by using the profile{self.profile} for aws_connection class")

        # key = key
        # key = f"{frequency('daily').define()}{self.context}/"

        data = client.get_object(Bucket=self.source,Key=f"{self.key}{self.filename}")
        self.file = pd.read_csv(data["Body"],sep=sep['standard'],encoding=encoding['standard']) 
        

        self.pipeline_id = uuid.uuid4().hex[:16]
        identity = f"{self.pipeline}_{self.pipeline_id}"
        
        logging.info(f"Adding new metadata fields")
        self.file['identity'] = identity
        self.file[f"loaded_{self.target}_date"] = date.today()
        self.file[f"loaded_{self.target}_time"] = datetime.now().time()
        self.file['tags'] = tags[self.pipeline]
        
        buffer = io.StringIO()
        self.file.to_csv(buffer,sep=sep['standard'],encoding=encoding['standard'],index=None)
        logging.info(f"Dataframe was successfully buffered")
                
        client.put_object(ACL='private',
        Body=buffer.getvalue(),
        Bucket=self.target,
        Key=f'{self.key}{self.filename}')
        logging.info(f"Done! Pipeline ran as {identity} using tags {tags[self.pipeline]}")

    def s3_to_dataframe(self):
        '''
        transfer method aims to move data from one S3 bucket to another, adding 
        new fields.
        4 fields are added by default:
            identity: Field which contains the pipeline name and pipeline_id
            loaded_{*Target bucket name}_date: Date of data uploading
            loaded_{*Target bucket name}_time: Time of data uploading
            tags: Tags that are binded to pipeline name
            
        To perform the transfer method, write as bellow:
        
        data_transfer(source='landing',
                target='processed',
                provider='s3',
                profile='admin',
                file=pd.DataFrame(),
                pipeline='ppl_fundamentus').transfer()
                
        This function will get fundametus.csv file from S3 source to target and also will
        add the four metadata fields mentioned before.
        
        '''        
        client = aws_connection('admin','s3').conn()         

        data = client.get_object(Bucket=self.source,Key=f"{self.key}{self.filename}")
        df = pd.read_csv(data["Body"],sep=sep['standard'],encoding=encoding['standard']) 

        return df



class api_get():
    '''
    This class aims to fetch data from api sources or scrappe it from open data sources:
    the main parameters are [url, headers]
    
    methods:
    fundamentus_df: Performs fetching data from https://www.fundamentus.com.br/resultado.php and then convertting that into Pandas DataFrame. 
    '''        
    def __init__(self,url:str,headers:dict):
        self.url = url
        self.headers = headers
        
    def fundamentus_df(self):
        
        '''
        This method performs fetching data from php table, iterates over it and then 
        converts it into Pandas DataFrame:        
               
        '''
        req = Request(self.url, headers = self.headers)
        response = urlopen(req)
        html = response.read()
        soup = BeautifulSoup(html, 'html.parser')    
    
        colunas_names = [col.getText() for col in soup.find('table', {'id': 'resultado'}).find('thead').findAll('th')]
        colunas = {i: col.getText() for i, col in enumerate(soup.find('table', {'id': 'resultado'}).find('thead').findAll('th'))}

        dados = pd.DataFrame(columns=colunas_names)
        
        for i in range(len(soup.find('table', {'id': 'resultado'}).find('tbody').findAll('tr'))):
            linha = soup.find('table', {'id': 'resultado'}).find('tbody').findAll('tr')[i].getText().split('\n')[1:]
            inserir_linha = pd.DataFrame(linha).T.rename(columns=colunas)
            dados = pd.concat([dados, inserir_linha], ignore_index=True)
            
        return dados
    

class pandas_to_s3():
    
    def __init__(self,file,target,key,filename,profile,provider):
        self.file       = file
        self.target     = target
        self.key        = key
        self.filename   = filename
        self.profile    = profile
        self.provider   = provider
        
    def transfer(self):
        
        client = aws_connection(profile=self.profile,provider=self.provider).conn()
        
        df = self.file
        
        # df.to_csv(self.target,sep=';',encoding='utf-8')
        buffer=io.StringIO()
        df.to_csv(buffer,sep=';',encoding='utf-8',index=None)
    
        client.put_object(ACL='private',
                Body=buffer.getvalue(),
                Bucket=self.target,
                Key=f"{self.key}{self.filename}")

class api_get_DFPS:   
       
    def get_file_list(): 
        
        url = 'https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/DFP/DADOS/'
        html_text = requests.get(url).text
        soup = BeautifulSoup(html_text, 'html.parser') 
         
        url_list = []
        files_text = soup.get_text()
        file_list = re.findall(r'\b\w+\.zip\b',files_text)    
      
        for i in file_list:
            ul = f'https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/DFP/DADOS/{i}'
            url_list.append(ul)         
            
        return url_list[-1]

    def parse_url():
        
        url = api_get_DFPS.get_file_list()
        response = requests.get(url) 
        resp = response.content
        bytesbuffer = io.BytesIO()
        bytesbuffer.write(response.content)
        zipped = zipfile.ZipFile(bytesbuffer)
        file_list = zipped.namelist()
    
        return resp

    def api_resp_to_s3(api_resp,target_bucket,key,provider,profile):
        '''
        This function aims to get api response into a given s3 bucket. 
        For achieve this, it writes a zip file from api source into temporary
        directory, extract it all into z_files folder, then finnaly decode it as utf-8
        and writes into s3 bucket as csv format.
        
        3 arguments are required:
        
        The api_resp argument comes from parse_url() function\n
        The target argument comes from path_name()\n
        The key argument comes from path_name()\n
        
        calling example:
        api_resp_to_s3(parse_url(),params['s3_target'],params['key'])   
        '''  
        
        params = path_name(            
        enviroment='dev',
        project='okonomikus',
        layer='landing',
        key=frequency('daily').define(),
        provider='s3',
        profile='admin',
        source='landing',
        target='landing').define() 
           
        
        credentials = aws_connection(profile=profile,provider=provider).account
        
        client = boto3.client(provider,                              
                              aws_access_key_id     =   credentials['aws_access_key_id'],
                              aws_secret_access_key =   credentials['aws_secret_access_key']
                        )
            
        with TemporaryDirectory() as temp_dir:
            
            # setting up the temp paths which are used for store temp files
            data = Path(temp_dir) / 'source.zip'
            root = Path(temp_dir) 
            z_files = Path(temp_dir) / 'z_files/'
            
            
            with open(data,'wb') as file:
                file.write(api_resp)
                with zipfile.ZipFile(data) as zipped:
                    zipped.extractall(z_files)
                                
                    for file in os.listdir(z_files):                
                        df = pd.read_csv(f"{z_files}/{file}",sep=';',encoding='cp1252')
                        # print(df.head(2))
                        buffer = io.StringIO()
                        df.to_csv(buffer,sep=';',encoding='utf-8',index=None)
                        
                        client.put_object(
                        Body=buffer.getvalue(),
                        Bucket=target_bucket,
                        Key=f'{key}{file}') 

class api_get_cad():

    def __init__(self,url_part:str,filenameURL:str,target:str,key:str,profile:str,provider:str):
        self.filenameURL    = filenameURL
        self.target         = target
        self.url_part       = url_part
        self.key            = key   
        self.profile        = profile
        self.provider       = provider     
        
    def s3_put_object(self):
        client = aws_connection(profile=self.profile,provider=self.provider).conn()
 
        url = f"{self.url_part}{self.filenameURL}"

        df = pd.read_csv(url,sep=';',encoding='Windows-1252')
        buffer = io.StringIO()    
        df.to_csv(buffer,encoding='utf-8',sep=';',index=None)
        
        client.put_object(ACL='private',
                    Body=buffer.getvalue(),
                    Bucket=self.target,
                    Key=self.key)                       

class logger_elt():
    
    def find_file_format(string:str):

        if len(re.findall(r'\.csv|\.txt|\.json', string)) > 0 :
            regex = re.findall(r'\.csv|\.txt|\.json', string)
            logging.info(f"Parsing the {regex} file.")
        else:
            logging.info(f"Parsing unknown file extension.")
        
    def iterating_over_file_list(file_list:list):
        '''
        logger_elt.iterating_over_file_list(filelist)
        '''
        
        lenght = 0
        for fl in file_list:
            logger_elt.find_file_format(str(fl))
            lenght += 1
            
        logging.info(f"{lenght} files was fetched!")

filelist = ['sfbsfk.csv','sfbsfk.txt','sfbsfk.json',45,'bbi','156']


            


# class transfer_old:                
#     def s3_upload_file_iterate_source(file_source,provider,profile): 
#         '''
#         This method iterates over a given directory and writes all files into 
#         a given s3 bucket.
        
#         '''      
#         WINDOWS_LINE_ENDING = b'\r\n'
#         UNIX_LINE_ENDING = b'\n'
        
#         params = path_name(            
#         enviroment='dev',
#         project='okonomikus',
#         layer='landing',
#         key=frequency('daily').define(),
#         provider='s3',
#         profile='admin',
#         source='landing',
#         target='landing').define()  
        
#         credentials = aws_connection(profile=profile).account
            
#         client = boto3.client(provider,                              
#                                 aws_access_key_id     =   credentials['aws_access_key_id'],
#                                 aws_secret_access_key =   credentials['aws_secret_access_key']
#                         )
        
#         for filename in os.listdir(file_source):
#             f = os.path.join(file_source, filename)
#             with open(f, 'rb') as open_file:
#                 content = open_file.read()
#                 content = content.replace(WINDOWS_LINE_ENDING, UNIX_LINE_ENDING)
            
#             if os.path.isfile(f):
#                 df = pd.read_csv(f,sep=';',encoding='Windows-1252')
#                 buffer=io.StringIO()
#                 df.to_csv(buffer,sep=';',encoding='utf-8',index=None)
                
#                 client.put_object(
#                     Body=buffer.getvalue(),
#                     Bucket=target,
#                     Key=f'{params['key']}{filename}')
                        
