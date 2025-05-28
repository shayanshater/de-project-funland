
#using EventBridge to run lambda every 15 min
resource "aws_cloudwatch_event_rule" "schedule" {
  name        = "ingest lammbda schedule"
  description = "Run the ingestion Lambda every 15 minutes"
  schedule_expression = "rate(15 minutes)"
}

#we give permission to trigger the lambda
resource "aws_lambda_permission" "allow_cloudwatch" {
  statement_id  = "AllowExecutionFromCloudWatch"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.extract_lambda_handler.function_name
  principal     = "events.amazonaws.com"
  source_arn    =aws_cloudwatch_event_rule.schedule.arn

}

#Attach the event to the Lambda
resource "aws_cloudwatch_event_target" "yada" {
  target_id = "LambdaTarget"
  rule      = aws_cloudwatch_event_rule.schedule.name
  arn       = aws_lambda_function.extract_lambda_handler.arn
}


