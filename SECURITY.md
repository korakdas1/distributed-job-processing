# Security

This repository implements a **job processing platform with explicit security controls**. Those controls are not a compliance certification (not SOC 2, ISO 27001, PCI DSS, HIPAA, GDPR, or OWASP certification).

## Supported state

Current security controls:

- static VIEWER and OPERATOR API keys (`Authorization: Bearer`)
- process-local rate limiting
- TLS termination at the optional nginx secure profile
- loopback-safe network defaults
- container hardening on application services

There is no user account database, no JWT issuer, and no production cloud deployment.

Known limitations include static keys, a process-local limiter, self-signed local TLS, and no WAF. See the README security model.

## Reporting a vulnerability

Do not file a public issue that includes exploit details, credentials, or private keys.

Send a security report **privately to the repository owner** through an appropriate private channel (for example a private message or email you already use with the owner). There is no dedicated security mailbox configured in this repository.

## Development vs secure profile

Ordinary development:

```bash
docker-compose up -d --build
```

HTTP on loopback, authentication off by default.

Secure production-oriented profile:

```bash
docker-compose -f docker-compose.yml -f docker-compose.prod.yml up -d --build
```

Requires generated secret files and a TLS certificate. See the README secure-profile section.

```bash
python scripts/generate_secrets.py
sh scripts/generate_tls.sh
```
