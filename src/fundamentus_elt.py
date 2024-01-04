import pandas as pd
import objects



def fundamentus_ingestion():
    
    params = objects.path_name(            
        enviroment='dev',
        project='okonomikus',
        layer='landing',
        key=objects.frequency('daily').define(),
        provider='s3',
        profile='admin',
        source='landing',
        target='landing',
        context='fundamentus').define()   

    objects.pandas_to_s3(
        
        file=objects.api_get(
        url='https://www.fundamentus.com.br/resultado.php',
        headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/76.0.3809.100 Safari/537.36'}).fundamentus_df(),
        profile=params['profile'],
        provider=params['provider'],
        target=params['s3_target'],
        key=params['key'],
        filename='fundamentus_historico.csv'
        ).transfer()

def fundamentus_to_processed():
    
    target_path = objects.path_name(            
        enviroment='dev',
        project='okonomikus',
        layer='landing',
        key=objects.frequency('daily').define(),
        provider='s3',
        profile='admin',
        source='landing',
        target='processed',
        context='fundamentus',        
        file_name='fundamentus_historico.csv').define()  
    # print(f"{target_path['s3_source']}{target_path['key']}{target_path['filename']}")
    # print(f"{target_path['key']}{target_path['filename']}")
    objects.data_transfer(
                    source=target_path['s3_source'],
                    target=target_path['s3_target'],
                    provider=target_path['provider'],
                    profile=target_path['profile'],
                    file=pd.DataFrame(),
                    pipeline='ppl_fundamentus',
                    key=f"{target_path['key']}",
                    filename=target_path['filename']
                    ).transfer()
    

def fundamentus_to_consume():
    
    target_path = objects.path_name(            
        enviroment='dev',
        project='okonomikus',
        layer='landing',
        key=objects.frequency('daily').define(),
        provider='s3',
        profile='admin',
        source='processed',
        target='consume',
        context='fundamentus',        
        file_name='fundamentus_historico.csv').define()  
    # print(f"{target_path['s3_source']}{target_path['key']}{target_path['filename']}")
    # print(f"{target_path['key']}{target_path['filename']}")
    objects.data_transfer(
                    source=target_path['s3_source'],
                    target=target_path['s3_target'],
                    provider=target_path['provider'],
                    profile=target_path['profile'],
                    file=pd.DataFrame(),
                    pipeline='ppl_fundamentus',
                    key=f"{target_path['key']}",
                    filename=target_path['filename']
                    ).transfer()
    
if __name__ == "__main__":
    
    fundamentus_ingestion()

    fundamentus_to_processed()

    # fundamentus_to_consume()
