import boto3
import json

# Thanks to https://hands-on.cloud/working-with-ecs-in-python-using-boto3/ for a good cheatsheet 

client = boto3.client("ecs", region_name="eu-central-1")

## create a new task in ecs

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
            "command": ["wgawronski-airflow-hybrid-demo","period1/temp.csv", "select * from customers WHERE location = \"Germany\"", "rds-airflow-hybrid","eu-central-1"],
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
    networkMode="bridge",
    requiresCompatibilities=["EXTERNAL"],
    cpu="256",
    memory="512"
)

print(json.dumps(response, indent=4, default=str))


# it will automatically use the latest version
# ideally you do not want this as this might impact idempotency
# so configure an explict version

new_taskdef=json.dumps(response['taskDefinition']['revision'], indent=4, default=str)
print("TaskDef is now at :" + str(new_taskdef))



#run task
# explicity set taskdef

response2 = client.run_task(
    cluster='test-hybrid',
    count=1,
    launchType='EXTERNAL',
    taskDefinition='test-external:{taskdef}'.format(taskdef=new_taskdef)
)

print(json.dumps(response2, indent=4, default=str)) 