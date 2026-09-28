variable "project" {
  description = "Tag applied to every resource; teardown checks for leftovers by it."
  type        = string
  default     = "distkv"
}

variable "region" {
  description = "AWS region."
  type        = string
  default     = "us-east-1"
}

variable "groups" {
  description = "Replication groups (a primary and a backup each)."
  type        = number
  default     = 3
}

variable "spares" {
  description = "Spare nodes that replace failed backups."
  type        = number
  default     = 1
}

variable "node_instance_type" {
  description = "Instance type for storage nodes and the coordinator. Fixed-performance (not burstable) so benchmarks do not depend on CPU credits."
  type        = string
  default     = "c7g.medium"
}

variable "loadgen_instance_type" {
  description = "Instance type for the load generator, which also builds the binaries. Larger than the nodes so the client is not the bottleneck."
  type        = string
  default     = "c7g.xlarge"
}

variable "max_lifetime_hours" {
  description = "Safety net: every instance shuts itself down (and, being set to terminate on shutdown, is deleted) this long after boot."
  type        = number
  default     = 4
}

variable "ssh_allowed_cidr" {
  description = "CIDR allowed to SSH in. Empty means the public IP of the machine running Terraform (/32)."
  type        = string
  default     = ""
}

variable "vpc_cidr" {
  type    = string
  default = "10.42.0.0/16"
}
