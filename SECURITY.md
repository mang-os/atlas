# Security

Atlas currently provides development-only HTTP Basic identities over localhost. It is not intended for an exposed production deployment. There is no production identity provider, TLS termination, tenant model or comprehensive secret rotation/retention policy.

Do not commit `.env`, tokens, passwords, private keys or raw infrastructure command logs. Generate credentials locally and keep infrastructure ports bound to loopback. Test fixture passwords are disposable and are not deployment credentials.

For a suspected vulnerability, use GitHub's private vulnerability reporting if enabled. If it is unavailable, open an issue asking for a private contact channel without including exploit details, secrets or sensitive data. Public issue templates are for non-sensitive bugs.
