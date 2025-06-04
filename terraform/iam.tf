# ---------------
# Lambda IAM Role
# ---------------

# Define role: allows lambda to assume this role
 data "aws_iam_policy_document" "trust_policy" {
   statement {
     effect = "Allow"
     principals {
       type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }

     actions = ["sts:AssumeRole"]
  }
}

# Create the role for lambda
resource "aws_iam_role" "lambda_role" {
  name_prefix        = "lambda-role-"
  assume_role_policy = data.aws_iam_policy_document.trust_policy.json
}

# ------------------------------
# Lambda IAM Policy to S3
# ------------------------------

# Define S3 document
data "aws_iam_policy_document" "s3_data_policy_doc" {
  statement {
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject",
      "s3:ListBucket"
    ]
    resources = [
      "${aws_s3_bucket.ingestion_bucket.arn}/*",
      "${aws_s3_bucket.ingestion_bucket.arn}/*"
    ]
  }
}

# Create S3 policy
resource "aws_iam_policy" "s3_write_policy" {
  name_prefix = "s3-policy-lambda-write"
  policy      = data.aws_iam_policy_document.s3_data_policy_doc.json 
}

# Attach s3 policy to the lambda role
resource "aws_iam_role_policy_attachment" "lambda_s3_write_policy_attachment" {
  role       = aws_iam_role.lambda_role.name
  policy_arn = aws_iam_policy.s3_write_policy.arn
}

# ------------------------------
# Lambda IAM Policy for CloudWatch
# ------------------------------

# Define cw doc. 
data "aws_iam_policy_document" "cw_document" {
  statement {
    effect = "Allow"
    actions = [
      "logs:CreateLogGroup",
      "logs:CreateLogStream",
      "logs:PutLogEvents",
      "logs:GetLogEvents",
      "logs:DescribeLogGroups",
      "logs:DescribeLogStreams",
      "logs:FilterLogEvents",
      "logs:StartQuery",
      "logs:GetQueryResults",
      "cloudwatch:PutMetricData",
      "cloudwatch:PutMetricAlarm",
      "cloudwatch:DescribeAlarms",
      "cloudwatch:DeleteAlarms"
    ]
    resources = ["arn:aws:logs:*"]
  }
}

# Create CloudeWatch policy
resource "aws_iam_policy" "cw_policy" {
  name   = "cw-lambda"
  policy = data.aws_iam_policy_document.cw_document.json
}

# Attach the cloudeWatch policy to the lambda role
resource "aws_iam_role_policy_attachment" "lambda_cw_policy_attachment" {
  role       = aws_iam_role.lambda_role.name
  policy_arn = aws_iam_policy.cw_policy.arn
}

# ------------------------------
# Lambda needs permission to use SSM to store last updated
# ------------------------------

# define ssm policy
data "aws_iam_policy_document" "ssm_lambda_policy_documentum"{
  statement {
    effect = "Allow"
    actions = [
        "ssm:PutParameter",
        "ssm:DeleteParameter",
        "ssm:GetParameterHistory",
        "ssm:GetParameter",
        ]
    resources = ["arn:aws:ssm:eu-west-2:${data.aws_caller_identity.current.account_id}:parameter/*"]
  }
}

# create ssm the policy
resource "aws_iam_policy" "ssm_lambda_policy" {
  name = "lambda-access-ssm-policy"
  description = "IAM policy for lambda to access ssm parameters"
  policy = data.aws_iam_policy_document.ssm_lambda_policy_documentum.json
}

# attach ssm policy to lambda role
resource "aws_iam_role_policy_attachment" "ssm_lambda_policy_attachment" {
  role = aws_iam_role.lambda_role.name
  policy_arn = aws_iam_policy.ssm_lambda_policy.arn
}

#-----------------------------
# IAM policy for lambda to get credentials | secrets from Secret Manager
#-----------------------------

# Define Iam role for Secret Manager
data "aws_iam_policy_document" "secretsmanager_lambda_policy_document" {
  statement { 
    effect = "Allow"
    actions = [
      "secretsmanager:CreateSecret",
      "secretsmanager:PutSecretValue",
      "secretsmanager:TagResource",
      "secretsmanager:DescribeSecret",
      "secretsmanager:GetSecretValue",
      "secretsmanager:UpdateSecret"
    ]
    resources = [
      "arn:aws:secretsmanager:${var.aws_region}:${data.aws_caller_identity.current.account_id}:secret:*"
    ]
  }
}

# Create Iam policy for secret manager
resource "aws_iam_policy" "secretsmanager_lambda_policy" {
  name = "lambda-secretmanager-access"
  policy = data.aws_iam_policy_document.secretsmanager_lambda_policy_document.json
}

# Attach secret manager policy to lambda
resource "aws_iam_role_policy_attachment" "lambda_secretsmanager_policy_attachment" {
  role       = aws_iam_role.lambda_role.name
  policy_arn = aws_iam_policy.secretsmanager_lambda_policy.arn
} 

# ------------------------------
# IAM role for Step Function to invoke Lambda
# ------------------------------

# Data for Step Function doc role
data "aws_iam_policy_document" "sf_role_document" {
  statement {
    effect = "Allow"
    principals {
      type        = "Service"
      identifiers = ["states.amazonaws.com"]
    }
    actions = ["sts:AssumeRole"]
  }
}

# Create State Machine role
resource "aws_iam_role" "step_function_role" {
  name_prefix        = "role-${var.step_function}-"
  assume_role_policy = data.aws_iam_policy_document.sf_role_document.json
}

# Define policy that allows Step Function to invoke lambdas
data "aws_iam_policy_document" "step_functions_document" {
  statement {
    effect = "Allow"
    actions = [
      "lambda:InvokeFunction"
    ]
    resources = [
      aws_lambda_function.extract_lambda_handler.arn,
      aws_lambda_function.transform_lambda_handler.arn,
      aws_lambda_function.load_lambda_handler.arn
      ]    
  }
}

# Create IAM policy for Step Function
resource "aws_iam_policy" "step_functions_policy" {
  name = "sf-${var.step_function}"
  policy = data.aws_iam_policy_document.step_functions_document.json
}

# attach the cw policy to the lambda role
resource "aws_iam_role_policy_attachment" "lambda_sf_policy_attachment" {
  role       = aws_iam_role.step_function_role.name
  policy_arn = aws_iam_policy.step_functions_policy.arn
}

#------------------------------
# IAM Policy for Lambda and Step Function to SNS | sending email alert
#------------------------------

# define the policy
data "aws_iam_policy_document" "sns_alert_document" {
  statement {
    effect = "Allow"
    actions = [ 
      "sns:CreateTopic",
      "sns:Publish",
      "sns:Subscribe"
     ]
    resources = [ aws_sns_topic.alerts.arn ]
  }
}

# create sns policy
resource "aws_iam_policy" "sns_policy" {
  name = "sns-notification-policy"
  policy = data.aws_iam_policy_document.sns_alert_document.json
}

# attach sns policy to lambda
resource "aws_iam_role_policy_attachment" "sns_attached_lambda" {
  role       = aws_iam_role.lambda_role.name
  policy_arn = aws_iam_policy.sns_policy.arn
}

# attach sns policy to step function
resource "aws_iam_role_policy_attachment" "sns_attached_sf" {
  role       = aws_iam_role.step_function_role.name
  policy_arn = aws_iam_policy.sns_policy.arn
}

# allow eventbridge to send SNS message
data "aws_iam_policy_document" "sns_topic_policy" {
  statement {
    effect = "Allow"
    principals {
      type        = "Service"
      identifiers = ["events.amazonaws.com"]
    }

    actions = [
      "SNS:Publish"
    ]

    resources = [
      aws_sns_topic.alerts.arn
    ]

    condition {
      test     = "ArnEquals"
      variable = "AWS:SourceArn"
      values   = [aws_cloudwatch_event_rule.lambda_failure_rule.arn]  # csak a cloudwatch ezen topicon
    }
  }
}



# ------------------------------
# IAM Policy for Scheduler to invoke step function
# ------------------------------
 
# Data for role
data "aws_iam_policy_document" "scheduler_role_document" {

  statement {
    effect = "Allow"
    principals {
      type        = "Service"
      identifiers = ["scheduler.amazonaws.com"]
    }
    actions = ["sts:AssumeRole"]
  }
}


# Create the role for sceduler
resource "aws_iam_role" "scheduler_role" {
  name        = "scheduler-role"
  assume_role_policy = data.aws_iam_policy_document.scheduler_role_document.json
}

# defining the policy document for cheduler
data "aws_iam_policy_document" "scheduler_policy_document" {
  statement {
    effect = "Allow"
    actions = ["states:StartExecution"]
    resources = [aws_sfn_state_machine.sfn_state_machine.arn]
  }
}

# Create IAM policy for scheduler 
resource "aws_iam_policy" "scheduler_policy" {
  name = "scheduler-policy"
  policy = data.aws_iam_policy_document.scheduler_policy_document.json
}

# Attach scheduler policy to step function
resource "aws_iam_role_policy_attachment" "scheduler_policy_attachment" {
  role       = aws_iam_role.scheduler_role.name
  policy_arn = aws_iam_policy.scheduler_policy.arn
}

