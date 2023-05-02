#Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
#SPDX-License-Identifier: Apache-2.0

from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.amazon.aws.operators.ecs import ECSOperator
from airflow.operators.python import PythonOperator
import boto3
import json


default_args = {
    'owner': 'ubuntu',
    'start_date': datetime(2019, 8, 14),
    'retry_delay': timedelta(seconds=60*60)
}

# Function that will take variables and create our new ECS Task Definition
def create_task(ti):
    client = boto3.client("ecs", region_name="eu-central-1")
    response = client.register_task_definition(
        containerDefinitions=[
            {
                "name": "airflow-hybrid-demo",
                "image": "174191956299.dkr.ecr.eu-central-1.amazonaws.com/hybrid-airflow:airflw",
                "cpu": 0,
                "portMappings": [],
                "essential": True,
                "environment": [],
                "mountPoints": [],
                "volumesFrom": [],
                "command": [
                    "wgawronski-airflow-hybrid-demo",
                    "period1/hq-data.csv",
                    "select * from customers WHERE location = \"Germany\"",
                    "rds-airflow-hybrid",
                    "eu-central-1"
                ],
                "logConfiguration": {
                    "logDriver": "awslogs",
                    "options": {
                        "awslogs-group": "/ecs/hybrid-airflow-cluster",
                        "awslogs-region": "eu-central-1",
                        "awslogs-stream-prefix": "ecs"
                    }
                }
            }
        ],
        taskRoleArn="arn:aws:iam::174191956299:role/ecs-anywhere-taskdef-hybridairflowApacheAirflowTas-ER38O4VR7EL8",
        executionRoleArn="arn:aws:iam::174191956299:role/ecs-anywhere-taskdef-hybridairflowApacheAirflowTas-ER38O4VR7EL8",
        family="test-external",
        networkMode="host",
        requiresCompatibilities=["EXTERNAL"],
        cpu="256",
        memory="512"
    )

    # we now need to store the version of the new task so we can ensure idemopotency

    new_taskdef=json.dumps(response['taskDefinition']['revision'], indent=4, default=str)
    print("TaskDef is now at :" + str(new_taskdef))
    return new_taskdef


with DAG('hybrid_airflow_dag_test', catchup=False, default_args=default_args, schedule_interval=None) as dag:
    create_taskdef = PythonOperator(
        task_id='create_taskdef',
        provide_context=True,
        python_callable=create_task,
        dag=dag
    )

    cloudquery = ECSOperator(
        task_id="cloudquery",
        dag=dag,
        cluster="hybrid-airflow-cluster",
        task_definition="test-external",
        overrides={},
        launch_type="EC2",
        awslogs_group="/ecs/hybrid-airflow-cluster",
        awslogs_stream_prefix="ecs"
    )

    # switch between these to change between remote and local MySQL
    # "command" : [ "wgawronski-airflow-hybrid-demo","period1/region-data.csv", "select * from customers WHERE location = \"Germany\"", "rds-airflow-hybrid","eu-central-1" ]}
    # "command" : [ "wgawronski-airflow-hybrid-demo","period1/region-data.csv", "select * from regionalcustomers WHERE country = \"Germany\"", "localmysql-airflow-hybrid","eu-central-1" ]}

    remotequery = ECSOperator(
        task_id="remotequery",
        dag=dag,
        cluster="hybrid-airflow-cluster",
        task_definition="test-external",
        launch_type="EXTERNAL",
        overrides={
            "containerOverrides": [
                {
                    "name": "airflow-hybrid-demo",
                    "command" : [
                        "wgawronski-airflow-hybrid-demo",
                        "period1/region-data.csv",
                        "select * from regionalcustomers WHERE country = \"Germany\"",
                        "localmysql-airflow-hybrid",
                        "eu-central-1"
                    ]
                }
            ]
        },
        awslogs_group="/ecs/hybrid-airflow-cluster",
        awslogs_stream_prefix="ecs",
    )

    create_taskdef >> cloudquery 
    create_taskdef >> remotequery