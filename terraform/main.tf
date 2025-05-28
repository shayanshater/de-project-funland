terraform {
  required_providers {
    aws={
      source="hashicorp/aws"
      version="~> 5.0"
    }
    
  }
  backend "s3" {
    bucket = "funland-terraform-configure-1-backend"
    key    = "folder/terraform state file"
    region = "eu-west-2"
  }

}

provider "aws" {
  region = "eu-west-2"
}
