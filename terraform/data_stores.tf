# ------------------------------------------------------------------------------
# PostgreSQL RDS Instance
# ------------------------------------------------------------------------------
resource "aws_db_instance" "postgres" {
  identifier        = "fin-pipeline-db-${var.environment}"
  engine            = "postgres"
  engine_version    = "15"
  instance_class    = "db.t4g.micro"
  allocated_storage = 20

  db_name  = "financial_data"
  username = "dataeng"
  password = var.db_password
  
  # Network and Security
  vpc_security_group_ids = [aws_security_group.rds_sg.id]
  db_subnet_group_name   = aws_db_subnet_group.default.name

  skip_final_snapshot = true # Set to false in Prod
  publicly_accessible = false
}

resource "aws_db_subnet_group" "default" {
  name       = "fin-pipeline-db-subnet-group-${var.environment}"
  subnet_ids = module.vpc.private_subnets
}

# ------------------------------------------------------------------------------
# ElastiCache Redis
# ------------------------------------------------------------------------------
resource "aws_elasticache_subnet_group" "redis" {
  name       = "fin-pipeline-redis-subnet-group-${var.environment}"
  subnet_ids = module.vpc.private_subnets
}

resource "aws_elasticache_cluster" "redis" {
  cluster_id           = "fin-pipeline-redis-${var.environment}"
  engine               = "redis"
  node_type            = "cache.t4g.micro"
  num_cache_nodes      = 1
  parameter_group_name = "default.redis7"
  port                 = 6379

  subnet_group_name  = aws_elasticache_subnet_group.redis.name
  security_group_ids = [aws_security_group.redis_sg.id]
}
