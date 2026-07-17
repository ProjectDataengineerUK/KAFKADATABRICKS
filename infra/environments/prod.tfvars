# Valores não sensíveis do ambiente prod.
# Variáveis sensíveis (confluent_cloud_api_*, mongodbatlas_*_key,
# mongodbatlas_app_password) são passadas via -var na linha de comando ou
# TF_VAR_* no shell, nunca versionadas aqui.

project_name = "consentpipeline"
environment  = "prod"
kafka_topic  = "consentimentos"
