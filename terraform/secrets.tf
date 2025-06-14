resource "aws_secretsmanager_secret" "db_credentials_secret" {
  name                    = "warehouse_totesys_credentials"
  description             = "database credentials for totesys and warehouse database"
  force_overwrite_replica_secret = true
  recovery_window_in_days = 0
}

resource "aws_secretsmanager_secret_version" "db_credentials_secret_version" {
  secret_id     = aws_secretsmanager_secret.db_credentials_secret.id
  secret_string = var.db_credentials
}