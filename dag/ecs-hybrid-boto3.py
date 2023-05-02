#Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
#SPDX-License-Identifier: Apache-2.0

from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
import boto3
import json

default_args = {
    'owner': 'ubuntu',
    'start_date': datetime(2019, 8, 14),
    'retry_delay': timedelta(seconds=60*60)
}

# Grab variables - fure improvement

#region
#taskRoleArn
#executionRoleArn
#family
#awslogs-group
#awslogs-stream-prefix
#task-name
#container-image
#command
#cluster


client = boto3.client("ecs", region_name="eu-central-1")

# Function that will take variables and create our new ECS Task Definition
def create_task(ti):
    response = client.register_task_definition(
        containerDefinitions=[
            {
                "name": "airflow-hybrid-boto3",
                "image": "174191956299.dkr.ecr.eu-central-1.amazonaws.com/hybrid-airflow:airflw",
                "cpu": 0,
                "portMappings": [],
                "essential": True,
                "environment": [],
                "mountPoints": [],
                "volumesFrom": [],
                "command": [
                    "wgawronski-airflow-hybrid-demo",
                    "period1/temp.csv",
                    "select * from customers WHERE country = \"Germany\"",
                    "rds-airflow-hybrid",
                    "eu-central-1"
                ],
                "logConfiguration": {
                    "logDriver": "awslogs",
                    "options": {
                        "awslogs-group": "/ecs/hybrid-airflow",
                        "awslogs-region": "eu-central-1",
                        "awslogs-stream-prefix": "ecs"
                    }
                }
            }
        ],
        taskRoleArn="arn:aws:iam::174191956299:role/ecs-anywhere-taskdef-hybridairflowApacheAirflowTas-ER38O4VR7EL8",
        executionRoleArn="arn:aws:iam::174191956299:role/ecs-anywhere-taskdef-hybridairflowApacheAirflowTas-ER38O4VR7EL8",
        family="test-external",
        networkMode="bridge",
        requiresCompatibilities=["EXTERNAL"],
        cpu="256",
        memory="512"
    )

    # we now need to store the version of the new task so we can ensure idemopotency

    new_taskdef=json.dumps(response['taskDefinition']['revision'], indent=4, default=str)
    print("TaskDef is now at :" + str(new_taskdef))
    return new_taskdef

# Function that will run our ECS Task
def run_task(ti):
    #new_taskdef=ti.xcom_pull(key='new_taskdef', task_ids=['create_taskdef'][0])
    new_taskdef=ti.xcom_pull(task_ids=['create_taskdef'][0])
    print("TaskDef passed is :" + str(new_taskdef))
    response2 = client.run_task(
        cluster='hybrid-airflow-cluster',
        count=1,
        launchType='EXTERNAL',
        taskDefinition='test-external:{taskdef}'.format(taskdef=new_taskdef)
)

with DAG('airflow_ecsanywhere_boto3', catchup=False, default_args=default_args, schedule_interval=None) as dag:
    first_task=PythonOperator(task_id='create_taskdef', python_callable=create_task, provide_context=True, dag=dag)
    second_task=PythonOperator(task_id='run_task', python_callable=run_task, provide_context=True, dag=dag)

    first_task >> second_task