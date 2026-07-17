variable "project_name" {
  description = "Nome curto do projeto, usado como prefixo dos recursos"
  type        = string
  default     = "consentpipeline"
}

variable "environment" {
  description = "Ambiente de deploy (dev ou prod)"
  type        = string

  validation {
    condition     = contains(["dev", "prod"], var.environment)
    error_message = "environment deve ser \"dev\" ou \"prod\"."
  }
}

variable "location" {
  description = "Região Azure onde os recursos serão provisionados"
  type        = string
  default     = "brazilsouth"
}

variable "kafka_bootstrap_servers" {
  description = "Endpoint do cluster Kafka (Confluent Cloud)"
  type        = string
  sensitive   = true
}

variable "kafka_api_key" {
  description = "API Key do Confluent Cloud"
  type        = string
  sensitive   = true
}

variable "kafka_api_secret" {
  description = "API Secret do Confluent Cloud"
  type        = string
  sensitive   = true
}

variable "mongo_uri" {
  description = "Connection string do MongoDB Atlas"
  type        = string
  sensitive   = true
}
