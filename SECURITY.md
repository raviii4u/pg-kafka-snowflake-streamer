# Security Policy

## Supported Versions

We actively maintain the latest version on the `main` branch.

## Reporting a Vulnerability

**Please do not report security vulnerabilities through public GitHub issues.**

If you discover a security vulnerability, please report it privately by opening a [GitHub Security Advisory](../../security/advisories/new) on this repository.

Include the following in your report:
- A description of the vulnerability
- Steps to reproduce
- Potential impact
- Any suggested mitigations (optional)

We will acknowledge your report within 48 hours and aim to release a fix within 14 days for critical issues.

## Scope

Security issues of interest include:

- Credentials or secrets exposed in code, logs, or error messages
- SQL injection or other injection vulnerabilities in Snowflake query generation
- Authentication or authorization bypasses on HTTP endpoints
- Sensitive data leakage via the CDC pipeline

## Out of Scope

- Security of the underlying infrastructure (Kafka, PostgreSQL, Snowflake, Redis) — these are the operator's responsibility
- Issues requiring physical access to the host

## Responsible Disclosure

We ask that you:
- Give us reasonable time to fix the issue before public disclosure
- Not exploit the vulnerability beyond what is necessary to demonstrate it
- Not access or modify data belonging to others

We are committed to acknowledging researchers who report valid vulnerabilities in our release notes.
