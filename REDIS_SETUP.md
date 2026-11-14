# Redis Setup for CDC Streamers

## Quick Start

Start Redis using Docker Compose:

```bash
docker-compose up -d redis
```

## Verify Redis is Running

```bash
# Check container status
docker ps | grep cdc-redis

# Connect to Redis CLI
docker exec -it cdc-redis redis-cli

# Test Redis connection
redis-cli ping
```

## Environment Variables

Set Redis connection details (optional, defaults provided):

```bash
export REDIS_HOST=localhost
export REDIS_PORT=6379
```

## DDL Storage

Each table DDL is stored using Schema.TableName as Redis key:
- Key format: `{schema}.{table}`
- Value: Complete DDL JSON with detailed column information

Example:
```
Key: public.users
Value: {
  "schema": "public",
  "table": "users", 
  "columns": [
    {
      "name": "id",
      "type": "integer",
      "snowflake_type": "INTEGER",
      "nullable": false,
      "default_value": null
    },
    {
      "name": "name",
      "type": "character varying",
      "snowflake_type": "VARCHAR(255)",
      "nullable": true,
      "max_length": 255
    }
  ],
  "last_updated": 1703123456789
}
```

## Redis Operations

View all table DDLs:
```bash
redis-cli KEYS "*.*"
```

Get specific table DDL:
```bash
redis-cli GET "public.users"
```

Delete table DDL:
```bash
redis-cli DEL "public.users"
```