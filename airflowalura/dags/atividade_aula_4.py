
from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator

def cumprimentos():
        print("Boas-vindas ao Airflow!")


## cabeçalho do schendule 
with DAG(
    'atividade-aula_4' ,
    start_date=days_ago(1),
    schedule_interval='@daily'
) as dag:

    ## definindo os operadores 
    tarefa_1 = PythonOperator(
        task_id='msg_tela',
        python_callable=cumprimentos
    )
