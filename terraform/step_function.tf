resource "aws_sfn_state_machine" "sfn_state_machine" {
  name     = var.step_function
  role_arn = aws_iam_role.step_function_role.arn

    definition = <<EOF
    {
  "StartAt": "Extract",
  "States": {
    "Extract": {
      "Type": "Task",
      "Resource": "arn:aws:states:eu-west-2:${data.aws_caller_identity.current.account_id}:function:${var.lambda_ingestion}",
      "Next": "Transform",
      "ResultPath": "$.myresult"
    },
    "Transform": {
      "Type": "Task",
      "Resource": "arn:aws:states:eu-west-2:${data.aws_caller_identity.current.account_id}:function:${var.lambda_ingestion}",
      "Next": "Load",
      "ResultPath": "$.myresult"
    },
    "Load": {
      "Type": "Task",
      "Resource": "arn:aws:states:eu-west-2:${data.aws_caller_identity.current.account_id}:function:${var.lambda_ingestion}",
      "ResultPath": "$.myresult",
    },
      "End": true
    }
  }
    EOF

}