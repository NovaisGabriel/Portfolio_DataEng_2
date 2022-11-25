import boto3
import datetime

from airflow.decorators import dag, task
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.models import Variable

aws_access_key_id = Variable.get("aws_access_key_id")
aws_secret_access_key = Variable.get("aws_secret_access_key")

client = boto3.client(
    'emr',
    region_name='us-east-2',
    aws_access_key_id = aws_access_key_id,
    aws_secret_access_key = aws_secret_access_key
)

default_args = {
    'owner': 'Gabriel',
    'start_date': datetime.datetime(2022, 11, 9),
    'depends_on_past': False
}

@dag(default_args = default_args,schedule_interval="@once", catchup=False, tags=['titanic', 'aws', 'boto3'])
def survivors():

    start = DummyOperator(task_id="Start")

    @task
    def emr_ingestion_titanic():
        newstep = client.add_job_flow_step(
            JobFlowId = "ID DA MÁQUINA",
            Steps = [
                {
                    'Name': 'It does the titanic data Ingestion',
                    'ActionOnFailure': 'CONTINUE', 
                    'HadoopJarStep': {
                        'Jar': 'command-runner.jar',
                        'Args':[
                            'spark-submit',
                            '--master', 'yarn',
                            '--deploy-mode', 'cluster',
                            '--packages', 'io.delta:delta-core2.12:2.1.0',
                            's3://teste-estrutura/codes/ingestion.py'
                        ]
                    }
                }
            ]
        )
        return newstep['StepIds'][0]
    
    @task
    def wait_emr_job(stepId):
        waiter = client.get_waiter('step_complete')
        waiter.wait(
            ClusterId = 'ID DA MÁQUINA',
            StepId = stepId,
            WaiterConfig = {
                'Delay':10,
                'MaxAttempts':300
            }
        )

    end = DummyOperator(task_id="fim")

    # orchestrate
    step1 = emr_ingestion_titanic()
    wait = wait_emr_job(step1)

    start >> step1
    wait >> end


exec = survivors()

