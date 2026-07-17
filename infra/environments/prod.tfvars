# Valores não sensíveis do ambiente prod.
# Variáveis sensíveis (kafka_*, mongo_uri) são injetadas via CI/CD
# (Azure DevOps variable group / pipeline secrets), nunca versionadas aqui.

project_name = "consentpipeline"
environment  = "prod"
location     = "brazilsouth"
