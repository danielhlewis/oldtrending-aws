import boto3

cluster_arn = "arn:aws:ecs:us-east-1:768752711595:cluster/NewspaperCdkStack-DetectronClusterF336EE20-P7WSQ3Fb14iC"
task_arn = "arn:aws:ecs:us-east-1:768752711595:task-definition/NewspaperCdkStackTaskDefBB7FA958:6"
client = boto3.client('ecs')

response = client.list_tasks(cluster=cluster_arn)
print('{}'.format(response))

response = client.run_task(cluster=cluster_arn,
                          taskDefinition="NewspaperCdkStackTaskDefBB7FA958")

print('{}'.format(response))