provider "kubernetes" {
  config_path = var.kubeconfig_path
}

provider "helm" {
  kubernetes = {
    config_path = var.kubeconfig_path
  }
}

locals {
  application_namespace = "marketplace-${var.environment}"
}

resource "kubernetes_namespace_v1" "spark_operator" {
  metadata {
    name = "spark-operator"
    labels = {
      "pod-security.kubernetes.io/enforce" = "restricted"
    }
  }
}

resource "kubernetes_namespace_v1" "application" {
  metadata {
    name = local.application_namespace
    labels = {
      "marketplace.io/environment"         = var.environment
      "pod-security.kubernetes.io/enforce" = "restricted"
    }
  }
}

resource "helm_release" "spark_operator" {
  name             = "spark-operator"
  repository       = "https://kubeflow.github.io/spark-operator"
  chart            = "spark-operator"
  version          = "2.5.2"
  namespace        = kubernetes_namespace_v1.spark_operator.metadata[0].name
  create_namespace = false
  atomic           = true
  wait             = true

  values = [yamlencode({
    spark = {
      jobNamespaces = [local.application_namespace]
    }
    webhook = {
      enable = true
    }
  })]
}

resource "helm_release" "application" {
  name      = "marketplace-lakehouse"
  chart     = "${path.module}/../helm/marketplace-lakehouse"
  namespace = kubernetes_namespace_v1.application.metadata[0].name
  atomic    = true
  wait      = true

  set = [{
    name  = "image.repository"
    value = split("@", var.application_image)[0]
  }, {
    name  = "image.digest"
    value = split("@", var.application_image)[1]
  }, {
    name  = "environment"
    value = var.environment
  }]

  depends_on = [helm_release.spark_operator]
}
