# Lambda IAM Role
# ---------------

# Define role 
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

# Create
resource "aws_iam_role" "lambda_ingestion_role" {
  name_prefix        = 
  assume_role_policy =
}

 