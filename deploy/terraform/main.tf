locals {
  node_count = var.groups * 2 + var.spares
  ssh_cidr   = var.ssh_allowed_cidr != "" ? var.ssh_allowed_cidr : "${chomp(data.http.my_ip.response_body)}/32"
  azs        = slice(sort(tolist(data.aws_ec2_instance_type_offerings.node.locations)), 0, 3)

  # Node i (0-based): nodes 2g and 2g+1 form group g+1 (primary, backup).
  # The primary goes to AZ g % 3 and the backup to the next AZ, so a group
  # survives the loss of any one AZ and the three groups use all three AZs.
  # Spares are spread round-robin.
  nodes = [
    for i in range(local.node_count) : {
      id    = "n${i + 1}"
      group = i < var.groups * 2 ? floor(i / 2) + 1 : 0
      role  = i < var.groups * 2 ? (i % 2 == 0 ? "primary" : "backup") : "spare"
      az    = i < var.groups * 2 ? (floor(i / 2) + i % 2) % 3 : i % 3
    }
  ]
}

# ---- Lookups ------------------------------------------------------------------

data "http" "my_ip" {
  url = "https://checkip.amazonaws.com"
}

# AZs that offer both instance types (not every AZ has every type).
data "aws_ec2_instance_type_offerings" "node" {
  location_type = "availability-zone"
  filter {
    name   = "instance-type"
    values = [var.node_instance_type]
  }
}

data "aws_ec2_instance_type_offerings" "loadgen" {
  location_type = "availability-zone"
  filter {
    name   = "instance-type"
    values = [var.loadgen_instance_type]
  }
}

# Latest Ubuntu 24.04 LTS for arm64 (Graviton), published by Canonical.
data "aws_ami" "ubuntu" {
  most_recent = true
  owners      = ["099720109477"]
  filter {
    name   = "name"
    values = ["ubuntu/images/hvm-ssd-gp3/ubuntu-noble-24.04-arm64-server-*"]
  }
  filter {
    name   = "architecture"
    values = ["arm64"]
  }
}

# ---- Network --------------------------------------------------------------------
# Public subnets with strict security groups: no NAT gateway, which would cost
# money every hour just so private instances could reach apt mirrors.

resource "aws_vpc" "main" {
  cidr_block           = var.vpc_cidr
  enable_dns_hostnames = true
  tags                 = { Name = "${var.project}-vpc" }
}

resource "aws_internet_gateway" "main" {
  vpc_id = aws_vpc.main.id
  tags   = { Name = "${var.project}-igw" }
}

resource "aws_subnet" "public" {
  count                   = 3
  vpc_id                  = aws_vpc.main.id
  availability_zone       = local.azs[count.index]
  cidr_block              = cidrsubnet(var.vpc_cidr, 8, count.index)
  map_public_ip_on_launch = true
  tags                    = { Name = "${var.project}-public-${local.azs[count.index]}" }
}

resource "aws_route_table" "public" {
  vpc_id = aws_vpc.main.id
  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.main.id
  }
  tags = { Name = "${var.project}-public" }
}

resource "aws_route_table_association" "public" {
  count          = 3
  subnet_id      = aws_subnet.public[count.index].id
  route_table_id = aws_route_table.public.id
}

resource "aws_security_group" "cluster" {
  name        = "${var.project}-cluster"
  description = "DistKV: SSH from the operator, everything else only within the cluster"
  vpc_id      = aws_vpc.main.id

  ingress {
    description = "SSH from the operator"
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = [local.ssh_cidr]
  }

  # RESP (7000), node gRPC (17000), coordinator gRPC (9000), Redis baseline:
  # reachable only from other members of this security group.
  ingress {
    description = "All traffic between cluster instances"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    self        = true
  }

  egress {
    description = "Outbound (apt, git)"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

# ---- SSH key ---------------------------------------------------------------------
# A key generated for this cluster only. The private key is written to
# deploy/.generated (git-ignored) and is also held in the local Terraform
# state, which is git-ignored too.

resource "tls_private_key" "ssh" {
  algorithm = "ED25519"
}

resource "aws_key_pair" "ssh" {
  key_name   = "${var.project}-key"
  public_key = tls_private_key.ssh.public_key_openssh
}

resource "local_sensitive_file" "ssh_key" {
  content         = tls_private_key.ssh.private_key_openssh
  filename        = "${path.module}/../.generated/id_ed25519"
  file_permission = "0600"
}

# ---- Instances -------------------------------------------------------------------

resource "aws_instance" "node" {
  count                                = local.node_count
  ami                                  = data.aws_ami.ubuntu.id
  instance_type                        = var.node_instance_type
  subnet_id                            = aws_subnet.public[local.nodes[count.index].az].id
  vpc_security_group_ids               = [aws_security_group.cluster.id]
  key_name                             = aws_key_pair.ssh.key_name
  instance_initiated_shutdown_behavior = "terminate"
  user_data = templatefile("${path.module}/user_data.sh.tftpl", {
    role                 = "node"
    max_lifetime_minutes = var.max_lifetime_hours * 60
  })

  root_block_device {
    volume_type = "gp3"
    volume_size = 16
  }

  tags = {
    Name  = "${var.project}-${local.nodes[count.index].id}"
    Role  = local.nodes[count.index].role
    Group = tostring(local.nodes[count.index].group)
  }
}

resource "aws_instance" "coordinator" {
  ami                                  = data.aws_ami.ubuntu.id
  instance_type                        = var.node_instance_type
  subnet_id                            = aws_subnet.public[0].id
  vpc_security_group_ids               = [aws_security_group.cluster.id]
  key_name                             = aws_key_pair.ssh.key_name
  instance_initiated_shutdown_behavior = "terminate"
  user_data = templatefile("${path.module}/user_data.sh.tftpl", {
    role                 = "node"
    max_lifetime_minutes = var.max_lifetime_hours * 60
  })

  root_block_device {
    volume_type = "gp3"
    volume_size = 16
  }

  tags = { Name = "${var.project}-coordinator", Role = "coordinator" }
}

resource "aws_instance" "loadgen" {
  ami                                  = data.aws_ami.ubuntu.id
  instance_type                        = var.loadgen_instance_type
  subnet_id                            = aws_subnet.public[0].id
  vpc_security_group_ids               = [aws_security_group.cluster.id]
  key_name                             = aws_key_pair.ssh.key_name
  instance_initiated_shutdown_behavior = "terminate"
  user_data = templatefile("${path.module}/user_data.sh.tftpl", {
    role                 = "loadgen"
    max_lifetime_minutes = var.max_lifetime_hours * 60
  })

  root_block_device {
    volume_type = "gp3"
    volume_size = 24
  }

  tags = { Name = "${var.project}-loadgen", Role = "loadgen" }

  lifecycle {
    precondition {
      condition     = contains(tolist(data.aws_ec2_instance_type_offerings.loadgen.locations), local.azs[0])
      error_message = "The load generator instance type is not offered in ${local.azs[0]}."
    }
  }
}

# ---- Inventory for the deploy scripts ---------------------------------------------

resource "local_file" "inventory" {
  filename = "${path.module}/../.generated/inventory.json"
  content = jsonencode({
    region   = var.region
    ssh_user = "ubuntu"
    groups   = var.groups
    spares   = var.spares
    coordinator = {
      public_ip  = aws_instance.coordinator.public_ip
      private_ip = aws_instance.coordinator.private_ip
      az         = aws_instance.coordinator.availability_zone
    }
    loadgen = {
      public_ip  = aws_instance.loadgen.public_ip
      private_ip = aws_instance.loadgen.private_ip
      az         = aws_instance.loadgen.availability_zone
    }
    nodes = [
      for i, node in local.nodes : {
        id         = node.id
        role       = node.role
        group      = node.group
        public_ip  = aws_instance.node[i].public_ip
        private_ip = aws_instance.node[i].private_ip
        az         = aws_instance.node[i].availability_zone
      }
    ]
  })
}
