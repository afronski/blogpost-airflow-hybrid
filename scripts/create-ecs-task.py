#Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
#SPDX-License-Identifier: Apache-2.0

import boto3
import json


client = boto3.client("ecs", region_name="eu-central-1")

def create_task():
    response = client.register_task_definition(
        containerDefinitions=[
            {
                "name": "airflow-hybrid-boto3",
                "image": "public.ecr.aws/a4b5h6u6/beachgeek:latest",
                "cpu": 0,
                "portMappings": [],
                "essential": True,
                "environment": [],
                "mountPoints": [],
                "volumesFrom": [],
                "command": ["wgawronski-airflow-hybrid-demo","period1/temp.csv", "select * from customers WHERE country = \"Germany\"", "rds-airflow-hybrid","eu-central-1"],
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
        networkMode="HOST",
        requiresCompatibilities=["EXTERNAL"],
        cpu="256",
        memory="512"
    )
