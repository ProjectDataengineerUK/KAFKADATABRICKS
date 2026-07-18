terraform {
  required_version = ">= 1.7.0"

  required_providers {
    confluent = {
      source  = "confluentinc/confluent"
      version = "~> 2.6"
    }
    mongodbatlas = {
      source  = "mongodb/mongodbatlas"
      version = "~> 1.18"
    }
  }

  # Sem backend remoto: este projeto usa tier free em todos os provedores
  # (nada de conta Azure/AWS/GCP para hospedar state). O `terraform apply`
  # roda localmente (state fica em infra/*.tfstate, já no .gitignore) e é
  # aplicado uma única vez por ambiente — o CI só roda `fmt`/`validate`
  # (ver .github/workflows/ci-cd.yml). Ver README para o passo a passo.
}

provider "confluent" {
  cloud_api_key    = var.confluent_cloud_api_key
  cloud_api_secret = var.confluent_cloud_api_secret
}

provider "mongodbatlas" {
  public_key  = var.mongodbatlas_public_key
  private_key = var.mongodbatlas_private_key
}

# ---------------------------------------------------------------------------
# Confluent Cloud — environment + cluster Basic (trial) + tópico + service
# account dedicada ao pipeline
# ---------------------------------------------------------------------------

resource "confluent_environment" "this" {
  display_name = "${var.project_name}-${var.environment}"
}

resource "confluent_kafka_cluster" "this" {
  display_name = "${var.project_name}-${var.environment}"
  availability = "SINGLE_ZONE"
  cloud        = "AWS"
  region       = var.confluent_region

  basic {}

  environment {
    id = confluent_environment.this.id
  }
}

resource "confluent_service_account" "app" {
  display_name = "${var.project_name}-${var.environment}-app"
  description  = "Usado pelo producer (simulador do app do banco) e pelos jobs Databricks"
}

resource "confluent_role_binding" "app_cluster_admin" {
  principal   = "User:${confluent_service_account.app.id}"
  role_name   = "CloudClusterAdmin"
  crn_pattern = confluent_kafka_cluster.this.rbac_crn
}

resource "confluent_api_key" "app" {
  display_name = "${var.project_name}-${var.environment}-app-key"

  owner {
    id          = confluent_service_account.app.id
    api_version = confluent_service_account.app.api_version
    kind        = confluent_service_account.app.kind
  }

  managed_resource {
    id          = confluent_kafka_cluster.this.id
    api_version = confluent_kafka_cluster.this.api_version
    kind        = confluent_kafka_cluster.this.kind

    environment {
      id = confluent_environment.this.id
    }
  }
}

resource "confluent_kafka_topic" "consentimentos" {
  kafka_cluster {
    id = confluent_kafka_cluster.this.id
  }
  topic_name       = var.kafka_topic
  partitions_count = 3
  rest_endpoint    = confluent_kafka_cluster.this.rest_endpoint

  credentials {
    key    = confluent_api_key.app.id
    secret = confluent_api_key.app.secret
  }

  depends_on = [confluent_role_binding.app_cluster_admin]
}

# ---------------------------------------------------------------------------
# MongoDB Atlas — projeto + cluster free (M0) + usuário de aplicação
# ("base Susep" simulada)
# ---------------------------------------------------------------------------

resource "mongodbatlas_project" "this" {
  name   = "${var.project_name}-${var.environment}"
  org_id = var.mongodbatlas_org_id
}

resource "mongodbatlas_cluster" "this" {
  project_id = mongodbatlas_project.this.id
  name       = "${var.project_name}-${var.environment}"

  provider_name               = "TENANT"
  backing_provider_name       = "AWS"
  provider_region_name        = var.mongodbatlas_region
  provider_instance_size_name = "M0"
}

resource "mongodbatlas_database_user" "app" {
  project_id         = mongodbatlas_project.this.id
  username           = "${var.project_name}-app"
  password           = var.mongodbatlas_app_password
  auth_database_name = "admin"

  roles {
    role_name     = "readWrite"
    database_name = var.mongo_db_name
  }
}

resource "mongodbatlas_project_ip_access_list" "allow_all" {
  project_id = mongodbatlas_project.this.id
  cidr_block = "0.0.0.0/0"
  comment    = "Demo de portfolio - IPs dinamicos (Free Edition/Render), restringir depois"
}
