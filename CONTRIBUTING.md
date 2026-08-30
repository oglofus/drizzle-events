# Contributing to drizzle-events

Thank you for contributing to `@oglofus/drizzle-events`! Bug reports, documentation improvements, tests, and code contributions are welcome.

## Before you start

- Search existing issues and pull requests before opening a new one.
- For security vulnerabilities, follow [the security policy](SECURITY.md) instead of opening a public issue.
- Keep changes focused and explain the motivation for behavioral changes.

## Development setup

This repository uses pnpm. Enable Corepack if necessary, then install dependencies:

```bash
corepack enable
pnpm install
```

Useful commands:

```bash
pnpm run build       # Compile TypeScript and generate declarations
pnpm test            # Build and run the Node.js test suite
pnpm run lint        # Run ESLint
pnpm run format      # Format the repository with Prettier
```

The test suite uses Node's built-in test runner and includes coverage for the base utilities and SQLite, Cloudflare D1, and PostgreSQL managers.

## Making changes

1. Create a topic branch from `main`.
2. Make the smallest change that solves the problem.
3. Add or update tests when behavior changes.
4. Update the README or other documentation when the public API changes.
5. Run the build, tests, lint, and relevant formatting checks locally.
6. Open a pull request against `main` and describe the change, testing performed, and any compatibility considerations.

Please follow the existing TypeScript style and use clear, descriptive commit and pull-request titles.

## Pull requests

Pull requests should:

- Explain what changed and why.
- Include tests for bug fixes and new behavior where practical.
- Avoid unrelated refactoring.
- Keep the public API and generated package behavior in mind.
- Pass the repository's required checks before requesting review.

Maintainers may ask for revisions, additional tests, or documentation updates before merging.
