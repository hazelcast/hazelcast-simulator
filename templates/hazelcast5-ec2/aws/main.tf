
locals {
    settings = yamldecode(file("../inventory_plan.yaml"))
    private_key = file("../${local.settings.keypair.private_key}")
    public_key = file("../${local.settings.keypair.public_key}")
}

provider "aws" {
    profile = "default"
    region = local.settings.region
}

resource "aws_default_vpc" "vpc" {
  tags = {
    Name = "Default VPC"
  }
}

#resource "aws_vpc" "prod-vpc" {
#    cidr_block = "10.0.0.0/16"
#    enable_dns_support = "true" #gives you an internal domain name
#    enable_dns_hostnames = "true" #gives you an internal host name
#    enable_classiclink = "false"
#    instance_tenancy = "default"
#    
#    tags = {
#        Name = "prod-vpc"
#    }
#}

#resource "aws_internet_gateway" "my_vpc_igw" {
#  vpc_id = local.settings.vpc_id
#
#  tags = {
#    Name = "My VPC - Internet Gateway"
#  }
#}

resource "aws_key_pair" "keypair" {
    key_name   = "simulator-keypair-${local.settings.basename}"
    public_key = local.public_key
}

resource "aws_subnet" "subnet" {
    vpc_id                  = local.settings.vpc_id
    cidr_block              = local.settings.cidr_block
    availability_zone       = local.settings.availability_zone
    map_public_ip_on_launch = true
    tags = {
        Name = "Simulator Public Subnet ${local.settings.basename}"
    }
}

resource "aws_route_table" "route_table" {
    vpc_id = local.settings.vpc_id
    route {
        cidr_block = "0.0.0.0/0"
        gateway_id = local.settings.internet_gateway_id
    }

    tags = {
        Name = "Simulator Public Subnet Route Table ${local.settings.basename}"
    }
}

resource "aws_route_table_association" "route_table_association" {
    subnet_id       = aws_subnet.subnet.id
    route_table_id  = aws_route_table.route_table.id
}

# ========== nodes ==========================

# Currently there is a single placement group defined for all nodes/load generators.
# If you want to use placement_group, uncomment the commented out 'placementgroup'
# configuration in the nodes and loadgenerators sections.
resource "aws_placement_group" "cluster_placement_group" {
    name     = "simulator-placement-group-${local.settings.basename}"
    strategy = "cluster"
}

resource "aws_security_group" "node-sg" {
    name        = "simulator-security-group-node-${local.settings.basename}"
    description = "Security group for the node"
    vpc_id      = local.settings.vpc_id
    
    tags = {
        Name = "Simulator Node Security Group ${local.settings.basename}",
        Owner = local.settings.owner
    }
    
    ingress {
        description = "SSH"
        from_port   = 22
        to_port     = 22
        protocol    = "tcp"
        cidr_blocks = ["0.0.0.0/0"]
    }


    ingress {
        description = "iperf3_udp"
        from_port   = 3000
        to_port     = 3000
        protocol    = "udp"
        cidr_blocks = ["0.0.0.0/0"]
    }

    ingress {
        description = "iperf3_tcp"
        from_port   = 3000
        to_port     = 3000
        protocol    = "tcp"
        cidr_blocks = ["0.0.0.0/0"]
    }

    ingress {
        description = "Hazelcast"
        from_port   = 5701
        to_port     = 5801
        protocol    = "tcp"
        cidr_blocks = ["0.0.0.0/0"]
    }
  
    ingress {
        description = "Simulator"
        from_port   = 9000
        to_port     = 9001
        protocol    = "tcp"
        cidr_blocks = ["0.0.0.0/0"]
    }

    ingress {
        description = "Hazelcast-tpc"
        from_port   = 11000
        to_port     = 12000
        protocol    = "tcp"
        cidr_blocks = ["0.0.0.0/0"]
    }

    egress {
        from_port   = 0
        to_port     = 0
        protocol    = "-1"
        cidr_blocks = ["0.0.0.0/0"]
    }
}

resource "aws_instance" "nodes" {
    key_name                = aws_key_pair.keypair.key_name
    ami                     = local.settings.nodes.ami
    instance_type           = local.settings.nodes.instance_type
    count                   = local.settings.nodes.count
    availability_zone       = local.settings.availability_zone
    placement_group         = aws_placement_group.cluster_placement_group.name
    vpc_security_group_ids  = [ aws_security_group.node-sg.id ]
    subnet_id               = aws_subnet.subnet.id
    tenancy                 = local.settings.nodes.tenancy
    
    tags = {
        Name  = "Simulator Node ${local.settings.basename}"
        Owner = local.settings.owner
        "passthrough:ansible_ssh_private_key_file" = local.settings.keypair.private_key
        "passthrough:ansible_user" = local.settings.nodes.user
    }
}

output "nodes" {
    value = [aws_instance.nodes.*]
}

# ========== load generators ==========================

resource "aws_security_group" "loadgenerator-sg" {
    name        = "simulator-security-group-loadgenerator-${local.settings.basename}"
    description = "Security group for the loadgenerator"
    vpc_id      = local.settings.vpc_id
    
    tags = {
        Name = "Simulator Load Balancer Security Group ${local.settings.basename}",
        Owner = local.settings.owner
    }
    
    ingress {
        description = "SSH"
        from_port   = 22
        to_port     = 22
        protocol    = "tcp"
        cidr_blocks = ["0.0.0.0/0"]
    }


    ingress {
        description = "iperf3_udp"
        from_port   = 3000
        to_port     = 3000
        protocol    = "udp"
        cidr_blocks = ["0.0.0.0/0"]
    }

    ingress {
        description = "iperf3_tcp"
        from_port   = 3000
        to_port     = 3000
        protocol    = "tcp"
        cidr_blocks = ["0.0.0.0/0"]
    }

    ingress {
        description = "Hazelcast"
        from_port   = 5701
        to_port     = 5801
        protocol    = "tcp"
        cidr_blocks = ["0.0.0.0/0"]
    }
  
    ingress {
        description = "Simulator"
        from_port   = 9000
        to_port     = 9001
        protocol    = "tcp"
        cidr_blocks = ["0.0.0.0/0"]
    }

    egress {
        from_port   = 0
        to_port     = 0
        protocol    = "-1"
        cidr_blocks = ["0.0.0.0/0"]
    }
}

resource "aws_instance" "loadgenerators" {
    key_name                = aws_key_pair.keypair.key_name
    ami                     = local.settings.loadgenerators.ami
    instance_type           = local.settings.loadgenerators.instance_type
    count                   = local.settings.loadgenerators.count
    subnet_id               = aws_subnet.subnet.id
    availability_zone       = local.settings.availability_zone
    placement_group         = aws_placement_group.cluster_placement_group.name
    vpc_security_group_ids  = [ aws_security_group.loadgenerator-sg.id ]
    tenancy                 = local.settings.loadgenerators.tenancy
    tags = {
        Name  = "Simulator Load Generator ${local.settings.basename}"
        Owner = local.settings.owner
        "passthrough:ansible_ssh_private_key_file" = local.settings.keypair.private_key
        "passthrough:ansible_user" = local.settings.loadgenerators.user
    }
} 

output "loadgenerators" {
    value = [aws_instance.loadgenerators.*]
}

# ========== management center ==========================

resource "aws_security_group" "mc-sg" {
    name        = "simulator-security-group-mc-${local.settings.basename}"
    description = "Security group for the Management Center"
    vpc_id      = local.settings.vpc_id

    tags = {
        Name = "Simulator MC Security Group ${local.settings.basename}",
        Owner = local.settings.owner
    }

    ingress {
        description = "SSH"
        from_port   = 22
        to_port     = 22
        protocol    = "tcp"
        cidr_blocks = ["0.0.0.0/0"]
    }

    ingress {
        description = "Hazelcast"
        from_port   = 8080
        to_port     = 8080
        protocol    = "tcp"
        cidr_blocks = ["0.0.0.0/0"]
    }

    ingress {
        description = "Simulator"
        from_port   = 8443
        to_port     = 8443
        protocol    = "tcp"
        cidr_blocks = ["0.0.0.0/0"]
    }

    egress {
        from_port   = 0
        to_port     = 0
        protocol    = "-1"
        cidr_blocks = ["0.0.0.0/0"]
    }
}

resource "aws_instance" "mc" {
    key_name                = aws_key_pair.keypair.key_name
    ami                     = local.settings.mc.ami
    instance_type           = local.settings.mc.instance_type
    count                   = local.settings.mc.count
    subnet_id               = aws_subnet.subnet.id
    availability_zone       = local.settings.availability_zone
    vpc_security_group_ids  = [ aws_security_group.mc-sg.id ]

    tags = {
        Name  = "Simulator MC ${local.settings.basename}"
        Owner = local.settings.owner
        "passthrough:ansible_ssh_private_key_file" = local.settings.keypair.private_key
        "passthrough:ansible_user" = local.settings.mc.user
    }

    connection {
        type        = "ssh"
        user        = local.settings.mc.user
        private_key = file("../${local.settings.keypair.private_key}")
        host        = self.public_ip
    }

    provisioner "remote-exec" {
        inline = [
            "wget -q https://repository.hazelcast.com/download/management-center/hazelcast-management-center-5.0.tar.gz",
            "tar -xzvf hazelcast-management-center-5.0.tar.gz",
            "while [ ! -f /var/lib/cloud/instance/boot-finished ]; do echo 'Waiting for cloud-init...'; sleep 1; done",
            "sudo apt-get -y update",
            "sudo apt-get -y install openjdk-11-jdk",
            "nohup hazelcast-management-center-5.0/bin/start.sh  > mc.out 2>&1 &",
            "sleep 2"
        ]
    }
}

output "mc" {
    value = [aws_instance.mc.*]
}






