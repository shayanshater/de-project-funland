# Create SNS Topic to notify about Lambda failures
resource "aws_sns_topic" "alerts" {
  name = "funland-lambda-failure-topic"
}

# Subscribe an email address to the SNS topic
resource "aws_sns_topic_subscription" "email" {
  topic_arn = aws_sns_topic.alerts.arn
  protocol  = "email"
  endpoint  = var.notification_email
}

# EventBridge rule to catch Lambda function failure events
resource "aws_cloudwatch_event_rule" "lambda_failure_rule" {
  name        = "funland-lambda-failure-alerts"
  description = "Trigger alert when Lambda function fails"

  event_pattern = jsonencode({
    "source": ["aws.lambda"],
    "detail-type": ["Lambda Function Invocation Result - Failure"],
    "detail": {
      "functionName": [
        aws_lambda_function.extract_lambda_handler.function_name,
        aws_lambda_function.transform_lambda_handler.function_name,
        aws_lambda_function.load_lambda_handler.function_name
      ]
    }
  })
}

# Target to send failure events to SNS topic
resource "aws_cloudwatch_event_target" "send_to_sns" {
  rule      = aws_cloudwatch_event_rule.lambda_failure_rule.name
  target_id = "SendToSNS"
  arn       = aws_sns_topic.alerts.arn
}