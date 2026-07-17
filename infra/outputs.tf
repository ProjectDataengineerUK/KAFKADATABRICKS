output "kafka_bootstrap_servers" {
  description = "Valor para KAFKA_BOOTSTRAP_SERVERS (.env / secret scope Databricks)"
  value       = confluent_kafka_cluster.this.bootstrap_endpoint
}

output "kafka_api_key" {
  description = "Valor para KAFKA_API_KEY"
  value       = confluent_api_key.app.id
  sensitive   = true
}

output "kafka_api_secret" {
  description = "Valor para KAFKA_API_SECRET"
  value       = confluent_api_key.app.secret
  sensitive   = true
}

output "kafka_topic" {
  value = confluent_kafka_topic.consentimentos.topic_name
}

output "mongo_uri" {
  description = "Valor para MONGO_URI — monte com o usuário/senha de mongodbatlas_database_user.app"
  value       = "mongodb+srv://${mongodbatlas_database_user.app.username}:<senha>@${replace(mongodbatlas_cluster.this.connection_strings[0].standard_srv, "mongodb+srv://", "")}"
  sensitive   = true
}

output "mongodbatlas_project_id" {
  value = mongodbatlas_project.this.id
}
