output "resource_group_name" {
  value = azurerm_resource_group.this.name
}

output "storage_account_name" {
  value = azurerm_storage_account.this.name
}

output "raw_filesystem_url" {
  description = "URL abfss:// do filesystem 'raw' usado pelo Autoloader"
  value       = "abfss://${azurerm_storage_data_lake_gen2_filesystem.raw.name}@${azurerm_storage_account.this.name}.dfs.core.windows.net/"
}

output "key_vault_name" {
  value = azurerm_key_vault.this.name
}

output "databricks_workspace_url" {
  value = azurerm_databricks_workspace.this.workspace_url
}

output "databricks_secret_scope" {
  value = databricks_secret_scope.consent_pipeline.name
}
