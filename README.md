
# Backend pipeline for spin.oldtrending.com

This is the backend code that powers spin.oldtrending.com, a website that dramatically displays top headlines from 100 years ago.

The page itself is very simple HTML and vanilla javascript, but it requires a fair bit of processing on the backend to identify and prepare the images to be shown on any given day.

Two versions of this pipeline exist, one that packages the entire workflow into a single docker-compose project that can run locally on a system, and a second one that runs entirely on AWS.  This is the AWS version.  While either of the pipelines will work equally well, in practice, the local docker version is preferred, since it is basically free to run.  The AWS version costs a bit of money each day to maintain a ~4gb ECS image and to run an ec2 instance that includes a GPU.

Since it uses CDK, the entire project can be deployed onto AWS with a minimal amount of setup.

1) Create an RDS database
1) Create an rds_config.py file with the following information
db_host = "xxx.rds.amazonaws.com"
db_username = ""
db_password = ""
db_name = "manifest" 
2) Request a quota increase for G type ec2 instances to at least 1 (default is 0)
