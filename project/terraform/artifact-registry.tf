resource "google_artifact_registry_repository" "my-project" {
  repository_id = "data-engineering-zoomcamp"
  location      = var.GCP_COMPUTE_ENGINE_REGION
  format        = "docker"
}