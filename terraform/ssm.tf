resource "aws_ssm_parameter" "last_checked_parameter" {
  name  = "last_checked"
  type  = "String"
  value = "2010-01-01 18:09:00.668878"
  overwrite = true
}