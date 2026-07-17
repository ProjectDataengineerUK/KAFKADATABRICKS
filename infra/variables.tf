variable "project_name" {
  description = "Nome curto do projeto, usado como prefixo dos recursos"
  type        = string
  default     = "consentpipeline"
}

variable "environment" {
  description = "Ambiente lógico (dev ou prod) — separação por catálogo dentro do mesmo workspace Databricks Free Edition"
  type        = string

  validation {
    condition     = contains(["dev", "prod"], var.environment)
    error_message = "environment deve ser \"dev\" ou \"prod\"."
  }
}

variable "kafka_topic" {
  description = "Nome do tópico Kafka de eventos de consentimento"
  type        = string
  default     = "consentimentos"
}

# --- Confluent Cloud ---

variable "confluent_cloud_api_key" {
  description = "Cloud API Key (nível conta) do Confluent Cloud — gerada em Confluent Cloud Console > Administration > API keys"
  type        = string
  sensitive   = true
}

variable "confluent_cloud_api_secret" {
  description = "Cloud API Secret correspondente à confluent_cloud_api_key"
  type        = string
  sensitive   = true
}

variable "confluent_region" {
  description = "Região AWS do cluster Confluent Cloud Basic"
  type        = string
  default     = "us-east-1"
}

# --- MongoDB Atlas ---

variable "mongodbatlas_public_key" {
  description = "Public API Key do MongoDB Atlas (Organization Access Manager > API Keys)"
  type        = string
  sensitive   = true
}

variable "mongodbatlas_private_key" {
  description = "Private API Key correspondente à mongodbatlas_public_key"
  type        = string
  sensitive   = true
}

variable "mongodbatlas_org_id" {
  description = "ID da organização no MongoDB Atlas"
  type        = string
}

variable "mongodbatlas_region" {
  description = "Região do cluster M0 (free tier) no MongoDB Atlas"
  type        = string
  default     = "US_EAST_1"
}

variable "mongodbatlas_app_password" {
  description = "Senha do usuário de aplicação criado no cluster Mongo (usada para montar MONGO_URI)"
  type        = string
  sensitive   = true
}

variable "mongo_db_name" {
  description = "Nome do banco MongoDB usado pela aplicação"
  type        = string
  default     = "susep_simulado"
}
