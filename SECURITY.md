# Security Policy

## Supported Versions

| Version | Supported          |
| ------- | ------------------ |
| 0.4.x   | :white_check_mark: |
| <= 0.3.x | :x:               |

The SDK currently targets `net8.0`. Security fixes are validated against the .NET 8
target and the repository's hermetic test suite. Server compatibility is qualified
through the opt-in conformance suite against an explicitly selected Streamline image
or endpoint; there is no blanket guarantee for every server version.

## Reporting a Vulnerability

Please report security vulnerabilities to **security@streamlinelabs.dev**.

**Do NOT open public issues for security vulnerabilities.**

### What to Include

- Description of the vulnerability
- Steps to reproduce
- Potential impact
- Suggested fix (if any)

### Response Timeline

- **Acknowledgment**: Within 48 hours
- **Initial Assessment**: Within 5 business days
- **Fix Timeline**: Communicated after assessment

We follow responsible disclosure practices and will credit reporters (with permission) in our release notes.

## Security Best Practices

For production deployments, please review the [Streamline Security Documentation](https://github.com/streamlinelabs/streamline-docs).
