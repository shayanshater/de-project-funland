# Create SNS Topic to notify about Lambda failures
resource "aws_sns_topic" "alerts" {
  name = "lambda-failure-topic"
}

# Subscribe an email address to the SNS topic
resource "aws_sns_topic_subscription" "email" {
  topic_arn = aws_sns_topic.alerts.arn
  protocol  = "email"
  endpoint  = var.notification_email
}