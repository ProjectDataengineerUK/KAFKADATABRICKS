# Valores não sensíveis do ambiente dev.
# Variáveis sensíveis (kafka_*, mongo_uri) são injetadas via CI/CD
# (Azure DevOps variable group / pipeline secrets), nunca versionadas aqui.

project_name = "consentpipeline"
environment  = "dev"
location     = "brazilsouth"
