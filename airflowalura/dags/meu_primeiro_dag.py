#     ## definição da ordem das execuções
#     tarefa_1 >> [tarefa_2,tarefa_3]
#     tarefa_3 >> tarefa_4 

#     import pendulum
# from airflow.models import DAG
# from airflow.operators.empty import EmptyOperator
# from airflow.operators.bash import BashOperator

# with DAG(
#             'meu_primeiro_dag',
#             start_date=pendulum.today('UTC').add(days=-1),
#             schedule_interval='@daily'
# ) as dag:

from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash_operator import BashOperator

## cabeçalho do schendule 
with DAG(
    'meu_primeiro_dag2' ,
    start_date=days_ago(2),
    schedule_interval='@daily'
) as dag:
    
    ## definição dos operadores (o que vão fazer)
    tarefa_1 = EmptyOperator(task_id= 'tarefa_1') 
    tarefa_2 = EmptyOperator(task_id= 'tarefa_2') 
    tarefa_3 = EmptyOperator(task_id= 'tarefa_3') 
    tarefa_4 = BashOperator(
        task_id = 'cria_pasta' ,
        bash_command= 'mkdir -p "/home/enrico/Documentos/backup/AULA/Alura/Engenharia de dados/projeto_airflow/airflowalura/pasta={{data_interval_end}}"'
    )

tarefa_1 >> [tarefa_2,tarefa_3]
tarefa_3 >> tarefa_4