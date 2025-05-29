
#set up EventBridge to trigger lambda every 15 min
resource "aws_cloudwatch_event_rule" "schedule" {
  name        = "schedule-cloudewatch-trigger-ingestion-lambda"
  description = "Run the ingestion Lambda every 15 minutes"
  schedule_expression = "rate(15 minutes)"
}

# #connect EventBridge to the Lambda
# resource "aws_cloudwatch_event_target" "yada" {
#   target_id = "LambdaTarget"
#   rule      = aws_cloudwatch_event_rule.schedule.name
#   arn       = aws_lambda_function.extract_lambda_handler.arn
# }

#allow EventBridge to call lambda
resource "aws_lambda_permission" "allow_cloudwatch" {
  statement_id  = "AllowExecutionFromCloudWatch"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.extract_lambda_handler.function_name
  principal     = "events.amazonaws.com"
  source_arn    =aws_cloudwatch_event_rule.schedule.arn
}




