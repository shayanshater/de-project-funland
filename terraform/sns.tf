resource "aws_sns_topic" "user_updates" {
  name = "test-2"
}

data "aws_sns_topic" "example" {
  name = "test-2"
}

resource "aws_sns_topic_subscription" "user_updates_sqs_target" {
  topic_arn = data.aws_sns_topic.example.arn
  protocol  = "email"
  endpoint  = "hari.sapkota1@gmail.com"
}