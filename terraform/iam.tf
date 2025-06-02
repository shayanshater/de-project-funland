# Lambda IAM Role
# ---------------

#Define role: allows lambda to assume this role
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

# Create the role
resource "aws_iam_role" "lambda_role" {
  name_prefix        = "lambda-role-"
  assume_role_policy = data.aws_iam_policy_document.trust_policy.json
}

# Lambda IAM Policy for S3 Write
# ------------------------------

# Define

data "aws_iam_policy_document" "s3_data_policy_doc" {
  statement {
    #TODO: this statement should give permission to put objects in the data bucket
    effect = "Allow"
    actions = [
      "s3:PutObject"
    ]
    resources = [
      "${aws_s3_bucket.ingestion_bucket.arn}/*"
    ]
  }
}

# Create
resource "aws_iam_policy" "s3_write_policy" {
  name_prefix = "s3-policy-lambda-write"
  policy      = data.aws_iam_policy_document.s3_data_policy_doc.json     #TODO use the policy document defined above
}

# Attach
resource "aws_iam_role_policy_attachment" "lambda_s3_write_policy_attachment" {
    #TODO: attach the s3 write policy to the lambda role
  role       = aws_iam_role.lambda_role.name
  policy_arn = aws_iam_policy.s3_write_policy.arn
}

# ------------------------------
# Lambda IAM Policy for CloudWatch
# ------------------------------

# Define
data "aws_iam_policy_document" "cw_document" {
  statement {
    effect = "Allow"
    actions = [
      "logs:CreateLogGroup",
      "logs:CreateLogStream",
      "logs:PutLogEvents",
    ]

    resources = ["arn:aws:logs:*"]

  }
}

# Create
resource "aws_iam_policy" "cw_policy" {
  #TODO: use the policy document defined above
  name   = "cw-lambda"
  policy = data.aws_iam_policy_document.cw_document.json
}

# Attach
resource "aws_iam_role_policy_attachment" "lambda_cw_policy_attachment" {
  #TODO: attach the cw policy to the lambda role
  role       = aws_iam_role.lambda_role.name
  policy_arn = aws_iam_policy.cw_policy.arn
}

# ------------------------------
# IAM Policy for Step Function to invoke Lambda
# ------------------------------

# Data for role
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

# Create the role
resource "aws_iam_role" "step_function_role" {
  name_prefix        = "role-${var.step_function}-"
  assume_role_policy = data.aws_iam_policy_document.sf_role_document.json
}

# Define policy that allows the Step Function to invoke any lambdas

data "aws_iam_policy_document" "step_functions_document" {
  statement {
    effect = "Allow"
    actions = [
      "lambda:InvokeFunction"
    ]
    resources = [aws_lambda_function.extract_lambda_handler.arn,
                aws_lambda_function.transform_lambda_handler.arn,
                aws_lambda_function.load_lambda_handler.arn]    
  }
}

#Create IAM policy for Step Function

resource "aws_iam_policy" "step_functions_policy" {
  name = "sf-${var.step_function}"
  policy = data.aws_iam_policy_document.step_functions_document.json
}

# Attach
resource "aws_iam_role_policy_attachment" "lambda_sf_policy_attachment" {
  #TODO: attach the cw policy to the lambda role
  role       = aws_iam_role.step_function_role.name
  policy_arn = aws_iam_policy.step_functions_policy.arn
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

# Create the role
resource "aws_iam_role" "scheduler_role" {
  name        = "scheduler-role"
  assume_role_policy = data.aws_iam_policy_document.scheduler_role_document.json
}

#defining the policy document 
data "aws_iam_policy_document" "scheduler_policy_document" {
  statement {
    effect = "Allow"
    actions = ["states:StartExecution"]
    resources = [aws_sfn_state_machine.sfn_state_machine.arn]
  }
}

#Create IAM policy for scheduler 
resource "aws_iam_policy" "scheduler_policy" {
  name = "scheduler-policy"
  policy = data.aws_iam_policy_document.scheduler_policy_document.json
}

# Attach
resource "aws_iam_role_policy_attachment" "scheduler_policy_attachment" {
  role       = aws_iam_role.scheduler_role.name
  policy_arn = aws_iam_policy.scheduler_policy.arn
}