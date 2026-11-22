terraform {
  required_version = ">= 1.6.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.80"
    }
  }
}

provider "aws" {
  region = var.region
  # Every resource that takes tags gets these, root volumes included
  # (root_block_device inherits them), so check-empty.sh can find
  # anything left behind by the tag project=dkv.
  default_tags {
    tags = {
      project = "dkv"
      cluster = var.name
    }
  }
}
