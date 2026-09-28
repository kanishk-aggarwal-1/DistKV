output "coordinator_public_ip" {
  value = aws_instance.coordinator.public_ip
}

output "loadgen_public_ip" {
  value = aws_instance.loadgen.public_ip
}

output "nodes" {
  value = {
    for i, node in local.nodes : node.id => {
      role       = node.role
      group      = node.group
      az         = aws_instance.node[i].availability_zone
      public_ip  = aws_instance.node[i].public_ip
      private_ip = aws_instance.node[i].private_ip
    }
  }
}

output "ssh_allowed_cidr" {
  value = local.ssh_cidr
}
