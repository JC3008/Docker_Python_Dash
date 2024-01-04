
import objects
import pandas as pd


def ingestion_DFP():
    
    params = objects.path_name(            
    enviroment='dev',
    project='okonomikus',
    layer='landing',
    key=objects.frequency('daily').define(),
    provider='s3',
    profile='admin',
    source='landing',
    target='landing',
    context='dfp').define()  

    objects.api_get_DFPS.api_resp_to_s3(api_resp=objects.api_get_DFPS.parse_url(),
                                target_bucket=params['s3_target'],
                                key=params['key'],
                                provider=params['provider'],
                                profile=params['profile'])

def dfp_to_processed():
    
    params = objects.path_name(            
        enviroment='dev',
        project='okonomikus',
        layer='landing',
        key=objects.frequency('daily').define(),
        provider='s3',
        profile='admin',
        source='landing',
        target='processed',
        context='dfp').define()  
    
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
                        pipeline='ppl_dfp',                        
                        filename=fn).transfer()

def dfp_to_consume():
    
    params = objects.path_name(            
        enviroment='dev',
        project='okonomikus',
        layer='landing',
        key=objects.frequency('daily').define(),
        provider='s3',
        profile='admin',
        source='processed',
        target='consume',
        context='dfp').define()  
    
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
                        pipeline='ppl_dfp',                        
                        filename=fn).transfer()
    

if __name__ == "__main__":
    ingestion_DFP()
    
    dfp_to_processed()
    
    # dfp_to_consume()

 

