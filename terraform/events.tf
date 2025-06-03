
# #set up EventBridge to trigger lambda every 15 min
# resource "aws_cloudwatch_event_rule" "schedule_rule" {
#   name        = "schedule-cloudewatch-trigger-step-function"
#   description = "Run the step function every 15 minutes"
#   schedule_expression = "rate(2 minutes)"
# }

# #connect EventBridge to the Lambda
# resource "aws_cloudwatch_event_target" "schedule_target" {
#   target_id = "StepFunctionTarget"
#   rule      = aws_cloudwatch_event_rule.schedule_rule.name
#   arn       = aws_sfn_state_machine.sfn_state_machine.arn
#   role_arn  = aws_iam_role.step_function_role.arn
# }

resource "aws_scheduler_schedule" "step_function_scheduler" {  ##TODO change the name
  name       = "my-step-function-scheduler"
  #group_name = "step-function-group"

  flexible_time_window {
    mode = "OFF"
  }

  schedule_expression = "rate(2 minute)"

  target {
    arn      = aws_sfn_state_machine.sfn_state_machine.arn
    # role_arn = aws_iam_role.step_function_role.arn
    role_arn = aws_iam_role.schedule_role.arn
  }
}





