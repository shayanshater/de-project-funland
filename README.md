# Data Engineering Funland Team Project

## Overview

This repository contains the implementation of a data engineering pipeline designed to simulate an end-to-end ETL (Extract, Transform, Load) solution. The system ingests data from an operational database, sanitize and transforms the data into a star-schema data warehouse, and provides visualizations for business intelligence purposes.

The project is built on AWS cloud services, leverages infrastructure-as-code, incorporates best practices in cloud monitoring, error logging, security, and aims to demonstrate a production-like, resilient data platform.

## Objectives

    *   Follow clean code principles: structured repository layout, consistent file naming conventions, modular design, and PEP8-compliant codebase.
    *   Apply Agile development methodology, use project management tools such as Trello and Slack.
    *   Implement Continuous Integration / Continuous Deployment (CI/CD) pipelines using GitHub Actions and Makefiles to automate testing, security and deployment processes.
    *   Apply Test-Driven Development (TDD) approach to ensure reliable and maintainable code with test coverage.
    *   Provision infrastructure using Infrastructure-as-Code (IaC) with Terraform to enable repeatable, version-controlled cloud resource deployment.
    *   Develop data pipelines to extract data from a PostgreSQL relational database and ingest it into AWS S3-based data lakes.
    *   Sanitize, transform, and remodel ingested data into a star-schema data warehouse model using pandas and awswrangler.
    *   Load transformed data into an AWS-hosted data warehouse (e.g. Redshift or PostgreSQL warehouse).
    *   Implement robust monitoring, alerting, and centralized logging via AWS CloudWatch and SNS to ensure pipeline observability.
    *   Build visualizations and business intelligence dashboards using Tableau for business reporting and insights.


# Project Title - Adel

A brief description of what this project does and who it's for


## Tech Stack - Hari

**Client:** React, Redux, TailwindCSS

**Server:** Node, Express


## Installation - Sarah

Install my-project with npm

```bash
  npm install my-project
  cd my-project
```
    
## Usage/Examples  - Shayan

Firstly, activate your virtual environment

```bash
source venv/bin/activate
```

To use AWS services and infrastructure, sign up to a AWS account and create a IAM user. Once this is done, extract your AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY.

```bash
export AWS_ACCESS_KEY_ID=<your key id>
export AWS_SECRET_ACCESS_KEY=<your secret access key>
export AWS_DEFAULT_REGION=<your default region>
```

An aws parameter needs to be put into parameter store with a parameter name of "last_checked". This parameter is a date in the format "YYYY-MM-DD HH:MM:SS:ffffff". This date should be some date before 2019, to ensure that all the data gets extracted from the database initially.


Now your aws account is linked to your local terminal and you are ready to navigate to the terraform directory

```bash
cd terraform
```

In this directory, an initialisation is needed to download the required hashicorp version and to setup the location of the terraform state file remotely. To accomplish this, we run:

```bash
terraform init
```

Once this finished, we are ready to see a plan of the infrastructure and its availability:

```bash
terraform plan
```

Be sure that all the information looks correct, and we are ready to deploy!! Run:

```bash
terraform apply
```

All the infrastructure should be created (ingestion and processed buckets, ETL lambdas and a step function to facilitate them, alongside the necessary roles and cloudwatch logs and notification systems):

```console

Apply complete! Resources: 1 added, 0 changed, 1 destroyed.

Outputs:

notification_email = "<email to receive error notifications>"

```


To see the infrastructure, we can use AWS CLI to view our buckets:

```bash
aws s3 ls
```


example output:

```console
2025-05-28 10:24:59 <ingestion-bucket-name>
2025-05-28 10:24:59 <processed-bucket-name>
```

And checking the AWS console for our state machine we can see:

![Alt text](/images/SF_image.png "This is a image of the state machine after it has ran a ETL process.")




## Running Tests - Elisa

To run tests, run the following command

```bash
  npm run test
```


## Screenshots - Hari
AWS Step function and visualisation

![App Screenshot](https://via.placeholder.com/468x300?text=App+Screenshot+Here)


## Acknowledgements - Adel

 - [Awesome Readme Templates](https://awesomeopensource.com/project/elangosundar/awesome-README-templates)
 - [Awesome README](https://github.com/matiassingers/awesome-readme)
 - [How to write a Good readme](https://bulldogjob.com/news/449-how-to-write-a-good-readme-for-your-github-project)


## Authors - Sarah

- [@octokatherine](https://www.github.com/octokatherine)




