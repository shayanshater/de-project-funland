# we are zipping the python code for the Lambda function
data "archive_file" "lambda" {
  type             = "zip"
  source_dir       = "${path.module}/../src/extract/lambda_handler"
  output_path      = "${path.module}/../deployment/handlers/extract.zip"
}


#we are creating the Lambda
resource "aws_lambda_function" "extract_lambda_handler" {
  # If the file is not in the current working directory you will need to include a
  # path.module in the filename.
  filename      = "${path.module}/../deployment/handlers/extract.zip"
  function_name = var.lambda_ingestion
  role          = aws_iam_role.lambda_role.arn
  handler       = extract.lambda_handler  
  runtime = var.python_runtime

  source_code_hash = data.archive_file.lambda.output_base64sha256

  environment {
    variables = {
      s3_bucket = aws_s3_bucket.ingestion_bucket.bucket
    #add the .env? 
    }
  }
}

resource "aws_lambda_layer_version" "lambda_layer" {
  filename   = "lambda_layer_payload.zip"
  layer_name = "lambda_layer_name"

  compatible_runtimes = ["nodejs20.x"]
}
