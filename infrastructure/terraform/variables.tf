variable "kubeconfig_path" {
  description = "Path to the target cluster kubeconfig. CI should inject this securely."
  type        = string
  sensitive   = true
}

variable "environment" {
  description = "Deployment environment name."
  type        = string
  validation {
    condition     = contains(["development", "staging", "production"], var.environment)
    error_message = "environment must be development, staging, or production"
  }
}

variable "application_image" {
  description = "Immutable application image by digest, for example registry/app@sha256:..."
  type        = string
  validation {
    condition     = can(regex("@sha256:[0-9a-f]{64}$", var.application_image))
    error_message = "application_image must be pinned by sha256 digest"
  }
}

