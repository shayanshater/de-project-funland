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
  name_prefix        = "role-${var.lambda_ingestion}-"
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
  name_prefix = "s3-policy-${var.lambda_ingestion}-write"
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
  name   = "cw-${var.lambda_ingestion}"
  policy = data.aws_iam_policy_document.cw_document.json
}

# Attach
resource "aws_iam_role_policy_attachment" "lambda_cw_policy_attachment" {
  #TODO: attach the cw policy to the lambda role
  role       = aws_iam_role.lambda_role.name
  policy_arn = aws_iam_policy.cw_policy.arn
}






 