
# Backend pipeline for oldtrending.com

This is the backend code that powers oldtrending.com, a website that dramatically displays top headlines from 100 years ago.

The page itself is very simple HTML and vanilla javascript, but it requires a fair bit of processing on the backend to identify and prepare the images to be shown on any given day.  A high level overview of the processing workflow is shown below:

![alt tag](https://i.imgur.com/rmBvTyn.png)

The end result is an S3 static site behind a cloudfront distribution.  Routing a custom domain name to it is outside the scope of the project, but is easy to do manually with Route 53.

Two versions of this pipeline exist, one that packages the entire workflow into a single docker-compose project that can run locally on a system, and a second one that runs entirely on AWS.  This is the AWS version.  While either of the pipelines will work equally well, in practice, the local docker version is preferred, since it is basically free to run.  The AWS version costs a bit of money each day (even on the free tier) to maintain a ~4gb ECS image and to run an ec2 with enough RAM to run the Detectron2 model.  Additionally, until a script to automatically clean up the S3 buckets is added, it's a good idea to manually clean up the s3 buckets periodically to avoid unnecessary storage charges.

Since it uses CDK, the entire project can be deployed onto AWS with a minimal amount of setup, but it currently requires a manually created RDS database.  To use it, you need to create an rds_config.py file in the get_pages_lambda folder with the following information

db_host = "xxx.rds.amazonaws.com"

db_username = ""

db_password = ""

db_name = "manifest"

To use, simply deploy the project from the command line with CDK.