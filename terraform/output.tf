# to see confirmation about the email
output "notification_email" {
  value       = var.notification_email
  description = "The email address receiving Lambda failure notifications"
  sensitive   = false
}