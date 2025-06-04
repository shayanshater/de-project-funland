# add scedule fo step function | state machine
resource "aws_scheduler_schedule" "trigger_15_min_intervals" {  
  name       = "my-step-function-scheduler"

  flexible_time_window {
    mode = "OFF"
  }

  schedule_expression = "rate(15 minutes)"  

  target {
    arn      = aws_sfn_state_machine.sfn_state_machine.arn
    role_arn = aws_iam_role.scheduler_role.arn
  }
}





