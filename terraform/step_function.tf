resource "aws_sfn_state_machine" "sfn_state_machine" {
  name     = "Funland-ETL"
  role_arn = aws_iam_role.iam_for_sfn.arn   #TODO: create a iam role for step function

    definition = <<EOF
    {
  "StartAt": "Load",
  "States": {
    "Load": {
      "Type": "Task",
      "Resource": "arn:aws:states:eu-west-2:${data.aws_caller_identity.current}:function:${var.lambda_ingestion}",
      "Next": "Transform",
      "ResultPath": "$.myresult"
    },
    "Transform": {
      "Type": "Task",
      "Resource": "YOUR-LAMBDA-ARN",
      "Next": "Load",
      "ResultPath": "$.myresult"
    },
    "Load": {
      "Type": "Task",
      "Resource": "YOUR-LAMBDA-ARN",
      "ResultPath": "$.myresult",
    },
      "End": true
    }
  }
}
    EOF

}