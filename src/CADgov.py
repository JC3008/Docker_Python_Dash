import objects
import pandas as pd

def cad_ingestion():    

    params = objects.path_name(            
    enviroment='dev',
    project='okonomikus',
    layer='landing',
    key=objects.frequency('daily').define(),
    provider='s3',
    profile='admin',
    source='landing',
    target='landing',
    context='cad',
    file_name='cad_cia_aberta.csv').define() 
    
    # print(f"{params['key']}{params['filename']}")
    
    objects.api_get_cad( 
    url_part='https://dados.cvm.gov.br/dados/CIA_ABERTA/CAD/DADOS/',
    filenameURL=params['filename'],
    target=params['s3_target'],    
    key=f"{params['key']}{params['filename']}",
    profile=params['profile'],
    provider=params['provider']).s3_put_object()
    
def cad_processed():
    
    params = objects.path_name(            
        enviroment='dev',
        project='okonomikus',
        layer='landing',
        key=objects.frequency('daily').define(),
        provider='s3',
        profile='admin',
        source='landing',
        target='processed',
        context='cad').define()  
    
    file_list = objects.s3_bucket_methods.listing_files_from_a_bucket(
        bucket=params['s3_source'],
        prefix=params['key'],
        profile=params['profile'],
        provider=params['provider'])
 
    for fn in file_list:
        
        objects.data_transfer(
                        source=params['s3_source'],
                        target=params['s3_target'],
                        provider=params['provider'],
                        profile=params['profile'],
                        file=pd.DataFrame(),
                        key=f"{params['key']}",
                        pipeline='ppl_cad',                        
                        filename=fn).transfer()

def cad_consume():
    
    params = objects.path_name(            
        enviroment='dev',
        project='okonomikus',
        layer='landing',
        key=objects.frequency('daily').define(),
        provider='s3',
        profile='admin',
        source='processed',
        target='consume',
        context='cad').define()  
    
    file_list = objects.s3_bucket_methods.listing_files_from_a_bucket(
        bucket=params['s3_source'],
        prefix=params['key'],
        profile=params['profile'],
        provider=params['provider'])
 
    for fn in file_list:
        
        objects.data_transfer(
                        source=params['s3_source'],
                        target=params['s3_target'],
                        provider=params['provider'],
                        profile=params['profile'],
                        file=pd.DataFrame(),
                        key=f"{params['key']}",
                        pipeline='ppl_cad',                        
                        filename=fn).transfer()
    

if __name__ == "__main__":
    cad_ingestion()
    
    cad_processed()
    
    # cad_consume()