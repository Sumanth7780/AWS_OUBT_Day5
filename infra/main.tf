provider "aws" {
  region = "us-east-1"
}

resource "aws_s3_bucket" "raw" {
  bucket = "nyc-raw-zone-dev"
}

resource "aws_db_instance" "mdm" {
  engine         = "postgres"
  instance_class = "db.t3.micro"
  allocated_storage = 20
  db_name        = "mdm"
  username       = "admin"
  password       = "Sumanth2325"
  skip_final_snapshot = true
}
