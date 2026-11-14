# Multi-Database CDC Deployment Guide

## Overview
This setup allows deploying multiple CDC agents from a single codebase, each configured for different PostgreSQL databases.

## Structure
```
k8s/
├── base/
│   └── deployment.yaml          # Template for all deployments
├── configs/
│   └── databases.yaml          # Database-specific configurations
scripts/
├── deploy.sh                   # Creates ConfigMaps and deployments
└── restart-deployments.sh      # Restarts all CDC deployments
```

## Configuration

### Adding New Database
Edit `k8s/configs/databases.yaml`:
```yaml
databases:
  your_new_db:
    project_name: "your-project"
    agent_name: "your-agent"
    postgres_host: "your-rds.amazonaws.com"
    postgres_database: "your_db"
    postgres_user: "your_user"
    table_whitelist: "public.table1,public.table2"
    replication_slot: "your_slot"
    publication_name: "your_pub"
    snowflake_schema: "YOUR_SCHEMA"
```

### Environment Variables Required
Set these in your CI/CD environment:
- `POSTGRES_PASSWORD`
- `SNOWFLAKE_URL`
- `SNOWFLAKE_USERNAME`
- `SNOWFLAKE_PASSWORD`
- `SNOWFLAKE_DATABASE`
- `SNOWFLAKE_WAREHOUSE`

## Pipeline Stages

1. **Build**: Compiles Java application
2. **Package**: Creates single Docker image
3. **Deploy**: Creates multiple deployments with database-specific configs
4. **Restart**: Ensures all deployments pick up latest image

## Manual Deployment
```bash
# Deploy to specific environment
./scripts/deploy.sh dev v1.0.0

# Restart all deployments
./scripts/restart-deployments.sh
```

## Monitoring
Each deployment has unique labels:
- `app=pg-kafka-snowflake-streamer`
- `agent=<agent-name>`

View all CDC deployments:
```bash
kubectl get deployments -l app=pg-kafka-snowflake-streamer
```