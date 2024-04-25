from airflow import DAG 
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.macros import ds_add
import pandas as pd 
from os.path import join 
from datetime import datetime, timedelta
import pendulum 

with DAG(
    'dados_climaticos2',
    start_date=pendulum.datetime(2024,4,2, tz='UTC'),
    schedule_interval= '0 0 * * 1', # executar toda segunda feira
) as dag:
    
    tarefa1 = BashOperator(
        task_id='cria_pasta',
        bash_command='mkdir -p "/home/enrico/Documentos/backup/AULA/Alura/Engenharia de dados/projeto_airflow/airflowalura/semana={{data_interval_end.strftime("%Y-%m-%d")}}"'
    )

    ##################################################3

    def extrai_dados(data_interval_end):
        city = 'Boston'
        key = 'UN53S3JS7UZKUUPHJHRXKXV7A'
        

        URL = join('https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/',
                    f'{city}/{data_interval_end}/{ds_add(data_interval_end,7)}?unitGroup=metric&include=days&key={key}&contentType=csv')

        dados = pd.read_csv(URL)

        file_path = f'/home/enrico/Documentos/backup/AULA/Alura/Engenharia de dados/projeto_airflow/airflowalura/semana={data_interval_end}/'

        dados.to_csv(file_path+ 'dados_brutos.csv')
        dados[['datetime', 'tempmin', 'temp', 'tempmax']].to_csv(file_path+'temperaturas.csv')
        dados[['datetime', 'description', 'icon']].to_csv(file_path+'condicoes.csv')


    tarefa2 = PythonOperator(
        task_id = 'extrai_dados',
        python_callable= extrai_dados,
        op_kwargs={'data_interval_end':'{{data_interval_end.strftime("%Y-%m-%d")}}'}
    )

tarefa1 >> tarefa2