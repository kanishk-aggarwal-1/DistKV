# Plans the configuration against mocked providers (no AWS account needed,
# nothing is created) and checks the layout invariants the design relies on.
#
#   terraform -chdir=deploy/terraform test

mock_provider "aws" {
  override_data {
    target = data.aws_ec2_instance_type_offerings.node
    values = { locations = ["us-east-1a", "us-east-1b", "us-east-1c", "us-east-1d"] }
  }
  override_data {
    target = data.aws_ec2_instance_type_offerings.loadgen
    values = { locations = ["us-east-1a", "us-east-1b", "us-east-1c"] }
  }
  override_data {
    target = data.aws_ami.ubuntu
    values = { id = "ami-0123456789abcdef0" }
  }
}

mock_provider "http" {
  override_data {
    target = data.http.my_ip
    values = { response_body = "203.0.113.7\n" }
  }
}

mock_provider "tls" {}
mock_provider "local" {}

run "default_layout" {
  command = plan

  assert {
    condition     = length(aws_instance.node) == 7
    error_message = "3 groups x 2 + 1 spare should give 7 nodes"
  }

  assert {
    condition     = jsonencode(local.azs) == jsonencode(["us-east-1a", "us-east-1b", "us-east-1c"])
    error_message = "the first three AZs offering the node type should be used"
  }

  assert {
    condition     = alltrue([for g in range(var.groups) : local.nodes[2 * g].az != local.nodes[2 * g + 1].az])
    error_message = "a group's primary and backup must be in different AZs"
  }

  assert {
    condition     = length(distinct([for n in local.nodes : n.az if n.role == "primary"])) == 3
    error_message = "the three primaries should be spread over three AZs"
  }

  assert {
    condition     = alltrue([for n in local.nodes : n.role == "spare" || n.group > 0])
    error_message = "every non-spare node belongs to a group"
  }

  assert {
    condition     = local.ssh_cidr == "203.0.113.7/32"
    error_message = "SSH must be limited to the operator's IP"
  }

  assert {
    condition = alltrue(concat(
      [for i in aws_instance.node : i.instance_initiated_shutdown_behavior == "terminate"],
      [aws_instance.coordinator.instance_initiated_shutdown_behavior == "terminate",
      aws_instance.loadgen.instance_initiated_shutdown_behavior == "terminate"]
    ))
    error_message = "the lifetime safety net relies on terminate-on-shutdown"
  }

  assert {
    condition = length([
      for rule in aws_security_group.cluster.ingress : rule
      if contains(rule.cidr_blocks == null ? [] : rule.cidr_blocks, "0.0.0.0/0")
    ]) == 0
    error_message = "nothing may be open to the whole internet"
  }
}

run "explicit_ssh_cidr_and_smaller_cluster" {
  command = plan

  variables {
    groups           = 2
    spares           = 0
    ssh_allowed_cidr = "198.51.100.0/24"
  }

  assert {
    condition     = length(aws_instance.node) == 4
    error_message = "2 groups and no spare should give 4 nodes"
  }

  assert {
    condition     = local.ssh_cidr == "198.51.100.0/24"
    error_message = "an explicit CIDR must win over auto-detection"
  }
}
