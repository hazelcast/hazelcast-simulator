
locals {
    settings = yamldecode(file("../inventory_plan.yaml"))
    private_key = file("../${local.settings.keypair.private_key}")
    public_key = file("../${local.settings.keypair.public_key}")
}

provider "aws" {
    profile = "default"
    region = local.settings.region
}

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
resource "aws_placement_group" "cluster_placement_group" {
    name     = "simulator-placement-group-${local.settings.basename}"
    strategy = "cluster"
}

resource "aws_security_group" "redis-sg" {
    name        = "simulator-security-group-redis-${local.settings.basename}"
    description = "Security group for the Redis"
    vpc_id      = local.settings.vpc_id
    
    tags = {
        Name = "Simulator Redis Security Group ${local.settings.basename}",
        Owner = local.settings.owner
    }
    
    ingress {
        description = "Redis"
        from_port   = local.settings.redis.port
        to_port     = local.settings.redis.port
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

resource "aws_elasticache_subnet_group" "redis_subnet_group" {
    name       = "Redis-Subnet-Group-${local.settings.basename}"
    subnet_ids = [aws_subnet.subnet.id]
}


resource "aws_elasticache_replication_group" "replication_group" {
    security_group_ids              = [ aws_security_group.redis-sg.id ]  
    subnet_group_name               = aws_elasticache_subnet_group.redis_subnet_group.name
    #preferred_cache_cluster_azs     = [ local.settings.availability_zone ]
    replication_group_id            = local.settings.redis.replication_group_id
    description                     = local.settings.redis.description
    node_type                       = local.settings.redis.node_type
    port                            = local.settings.redis.port
    parameter_group_name            = local.settings.redis.parameter_group_name
    auth_token                      = local.settings.redis.auth_token
    transit_encryption_enabled      = local.settings.redis.transit_encryption_enabled
    automatic_failover_enabled      = local.settings.redis.automatic_failover_enabled
    replicas_per_node_group         = local.settings.redis.replicas_per_node_group
    num_node_groups                 = local.settings.redis.num_node_groups

    # todo: doesn't work with password.
    provisioner "local-exec" {    
        command = "echo URI=redis://${self.configuration_endpoint_address}:${local.settings.redis.port} > ../simulator.properties"          
    }
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
    #placement_group        = local.settings.loadgenerators.placement_group
    vpc_security_group_ids  = [ aws_security_group.loadgenerator-sg.id ]
    tenancy                 = local.settings.loadgenerators.tenancy
    tags = {
        Name  = "Simulator Load Generator ${local.settings.basename}"
        Owner = local.settings.owner
        "passthrough:ansible_ssh_private_key_file" = local.settings.keypair.private_key
        "passthrough:ansible_user" = local.settings.loadgenerators.user
    }

    connection {
        type        = "ssh"
        user        = local.settings.loadgenerators.user
        private_key = local.private_key
        host        = self.public_ip
    }

    provisioner "remote-exec" {
        inline = [
           "echo \"${local.private_key}\" > ~/.ssh/id_booboo",
           "echo \"${local.public_key}\" > ~/.ssh/id_rsa.pub",
           "chmod 600 ~/.ssh/id_rsa*"
        ]
    }
} 

output "loadgenerators" {
    value = [aws_instance.loadgenerators.*]
}
