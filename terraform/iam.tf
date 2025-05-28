# Lambda IAM Role
# ---------------

# Define role: allows lambda to assume this role
data "aws_iam_policy_document" "lambda_trust_policy" {
  statement {
    effect = "Allow"

    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }

    actions = ["sts:AssumeRole"]
  }
}

# # Create
# resource "aws_iam_role" "lambda_ingestion_role" {
#   name_prefix        = 
#   assume_role_policy =
# }

 