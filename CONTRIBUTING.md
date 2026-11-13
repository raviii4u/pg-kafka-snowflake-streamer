# Contributing to pg-kafka-snowflake-streamer

Thank you for your interest in contributing. This document explains how to get started.

## Getting Started

### Prerequisites

- Java 21
- Docker and Docker Compose (for local dependencies)
- A PostgreSQL instance with logical replication enabled (`wal_level = logical`)
- Access to a Snowflake account for integration testing

### Run locally

Start the local dependencies (Kafka, Redis):

```bash
docker-compose up -d
```

Build the project:

```bash
./gradlew build -x test
```

Run tests:

```bash
./gradlew test
```

### Required PostgreSQL setup

Before running the application, create a replication slot and publication on your source database:

```sql
SELECT pg_create_logical_replication_slot('debezium_stream_local_slot', 'pgoutput');
CREATE PUBLICATION debezium_stream_local_publication FOR ALL TABLES;
```

## How to Contribute

### Reporting bugs

Open a GitHub issue with:
- A clear description of the problem
- Steps to reproduce
- Expected vs actual behaviour
- Relevant logs or stack traces

### Suggesting features

Open a GitHub issue tagged `enhancement`. Describe the use case and the proposed solution. For large changes, please discuss in an issue before opening a PR.

### Submitting pull requests

1. Fork the repository and create a branch from `main`
2. Make your changes
3. Run `./gradlew build` and ensure it passes clean
4. Write or update tests for any changed behaviour
5. Open a PR with a clear description of what changed and why

### Code style

- Follow the existing code style — no style linter is enforced but consistency is appreciated
- Keep commits focused and atomic
- Write meaningful commit messages

## Project structure

```
src/main/java/io/cdcstreamers/
  config/       Spring Boot configuration classes
  controller/   HTTP endpoints (health check)
  converter/    Custom Debezium type converters
  model/        Value objects (CdcRecord, CdcMode)
  service/      Core pipeline services
  util/         Stateless utility classes
docs/           Architecture documentation and articles
k8s/            Kubernetes deployment templates
scripts/        Deployment and YAML generation scripts
```

## Questions

Open a GitHub Discussion or file an issue tagged `question`.
