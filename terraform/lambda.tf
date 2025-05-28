# we are zipping the python code for the Lambda function
data "archive_file" "lambda" {
  type             = "zip"
#   source_file      = "${path.module}/../src/extract.py"
#   output_path      = "${path.module}/../files/extract.zip"

}


#we are creating the Lambda
resource "aws_lambda_function" "extract_lambda_handler" {
  # If the file is not in the current working directory you will need to include a
  # path.module in the filename.
  filename      = "${path.module}/../files/extract.zip"
  function_name = var.lambda_ingestion
  role          = aws_iam_role.lambda_role.arn
#   handler       = pythonfile.pythonfunction  (change this bit)

  source_code_hash = data.archive_file.lambda.output_base64sha256

  runtime = var.python_runtime

  environment {
    variables = {
      s3_bucket = aws_s3_bucket.ingestion_bucket.bucket
    }
  }
}
