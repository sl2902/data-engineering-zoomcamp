resource "google_compute_instance" "agent-vm" {
  name         = var.COMPUTE_ENGINE_NAME
  machine_type = var.COMPUTE_ENGINE_MACHINE_TYPE
  zone         = var.COMPUTE_ENGINE_REGION

  metadata = {
    "PREFECT_API_KEY" = var.PREFECT_API_KEY
    "PREFECT_API_URL" = var.PREFECT_API_URL
    "PREFECT_AGENT_QUEUE_NAME" = var.PREFECT_AGENT_QUEUE_NAME
  }

  metadata_startup_script = file("../utilities/setup_vm.sh")

  boot_disk {
    initialize_params {
      image = "ubuntu-2004-focal-v20230104"
    }
  }

  service_account {
    email = var.SERVICE_ACCOUNT_EMAIL
    scopes = [
      "cloud-platform",
    ]
  }

  network_interface {
    network = "default"
    access_config {
    }
  }
}