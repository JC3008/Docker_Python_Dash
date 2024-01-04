from objects import *
import pandas as pd
import plotly.express as px
from dash import Dash, html, dcc, callback, Output, Input

params = path_name(            
    enviroment='dev',
    project='okonomikus',
    layer='landing',
    key=frequency('daily').define(),
    provider='s3',
    profile='admin',
    source='processed',
    target='consume',
    context='dfp',
    file_name='dfp_cia_aberta_DRE_ind_2023.csv').define() 

client = aws_connection(profile='admin',provider='s3').conn()

filenames = [{'cad':'cad_cia_aberta.csv'}]
            #  {'dfp':'dfp_cia_aberta_DRE_con_2023.csv'},
            #  {'dfp':'dfp_cia_aberta_DRE_ind_2023.csv'},             
            #  {'fundamentus':'fundamentus_historico.csv'}]

n = 0
for i in filenames:
    for k,v in i.items(): 
        print(f"{k}/{v} was buffered as df_{n} dataframe.")   
        data = client.get_object(Bucket=params['s3_source'],Key=f"{frequency('daily').define()}{k}/{v}")
        exec(f"df_{n} = pd.read_csv(data['Body'],sep=sep['standard'],encoding=encoding['standard'])")
        n += 1
        
# pandas config altered for displaying all columns and rows 
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.option_context('display.multi_sparse', False)

# Slicing for getting only SIT=ATIVO
df_0 = df_0.loc[df_0['SIT']=='ATIVO']

# dupl column created for categorize duplicates CNPJ 
df_0['dupl'] = df_0['CNPJ_CIA'].duplicated()

# Retriving only TP_MERC = BOLSA
df_0 = df_0.loc[df_0['TP_MERC']=='BOLSA']

# Columns to drop
cols_to_drop = ['LOGRADOURO','COMPL','TEL_RESP','DDD_FAX_RESP','FAX_RESP',
                'CNPJ_AUDITOR','EMAIL_RESP','DDD_TEL_RESP','CEP_RESP','TEL',
                'PAIS','BAIRRO','DT_CANCEL','MOTIVO_CANCEL','SIT','CEP','DDD_TEL',
                'DDD_FAX','FAX','EMAIL','TP_RESP','RESP','DT_INI_RESP',
                'LOGRADOURO_RESP','COMPL_RESP','COMPL_RESP','BAIRRO_RESP',
                'MUN_RESP','UF_RESP','PAIS_RESP','identity',
                'loaded_de-okkus-processed-dev-727477891012_date',
                'loaded_de-okkus-processed-dev-727477891012_time','tags','dupl']

df_0 = df_0.drop(cols_to_drop, axis=1)

# incrementting dataframe with new columns
df_0['ANO_INI_SIT'] = pd.to_datetime(df_0['DT_INI_SIT']).dt.year
df_0['MES_INI_SIT'] = pd.to_datetime(df_0['DT_INI_SIT']).dt.month
df_0['count'] = 1
# slicing for shrink it
subscriptions_by_year = df_0[['ANO_INI_SIT','UF','count']]
lst = list(subscriptions_by_year['UF'])
subscriptions_by_year = subscriptions_by_year.groupby(['ANO_INI_SIT','UF']).sum().reset_index()
subscriptions_by_year = subscriptions_by_year.pivot(index='ANO_INI_SIT',columns='UF',values='count').fillna(0).reset_index()

external_stylesheets = []

app = Dash(__name__)

app.layout = html.Div(
    children=[html.H1('Número de iniciantes na categoria Bolsa. '),
              html.P('Ano x Estado de SP'),
            #   dcc.Dropdown(
            #       options=[
            #           {'label':'RJ','value':'RJ'},
            #           {'label':'SC','value':'SC'}
            #       ],
                  
            #   ),
              dcc.Graph(
                  config={'displayModeBar':False},
                  figure={
                      'data':[                          
                          {'x':subscriptions_by_year['ANO_INI_SIT'],'y':subscriptions_by_year['SP'],                           
                           'type':'line',
                           'name':'ano'},                          
                          ],
                      'layout': {
                          'title':'Evolução anual no estado de São Paulo',
                          'paper_bgcolor':'#FFFFFF',
                          'titlefont': {
                              'size':30,
                              'color':'#009c3b'
                          }
                          
                      }
                  }
              )]            
    )

if __name__ == '__main__':
    app.run_server(debug=False, host='0.0.0.0', port=8888)

# if __name__ == '__main__':
#     app.run_server(debug=True, host='127.0.0.1', port=8888)