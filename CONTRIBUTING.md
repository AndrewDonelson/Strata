# Contributing to Strata

Thank you for taking the time to contribute! The following guidelines help
keep the project consistent and the review process smooth.

## Table of Contents

- [Code of Conduct](#code-of-conduct)
- [Getting Started](#getting-started)
- [Development Setup](#development-setup)
- [Making Changes](#making-changes)
- [Commit Messages](#commit-messages)
- [Pull Requests](#pull-requests)
- [Reporting Bugs](#reporting-bugs)
- [Requesting Features](#requesting-features)
- [Code Style](#code-style)
- [Testing](#testing)
- [License](#license)

---

## Code of Conduct

This project is governed by the [Contributor Covenant Code of Conduct](CODE_OF_CONDUCT.md).
By participating you agree to abide by its terms.

---

## Getting Started

1. **Fork** the repository on GitHub.
2. **Clone** your fork locally:
   ```bash
   git clone https://github.com/<your-username>/strata.git
   cd strata
   ```
3. Add the upstream remote so you can pull future changes:
   ```bash
   git remote add upstream https://github.com/AndrewDonelson/strata.git
   ```

---

## Development Setup

### Prerequisites

| Tool | Minimum version |
|------|----------------|
| Go   | 1.21           |
| Docker | latest (for integration tests via Testcontainers) |
| Redis | 7+ (optional — tests spin one up automatically) |
| PostgreSQL | 15+ (optional — tests spin one up automatically) |

### Install dependencies

```bash
go mod download
```

### Run the full test suite

```bash
make test          # unit + white-box tests (no external services needed)
make test-integ    # requires Docker — spins up Redis + Postgres via Testcontainers
make all           # lint + vet + test + coverage report
```

---

## Making Changes

1. Create a **feature branch** from `main`:
   ```bash
   git checkout -b feat/my-feature
   ```
2. Make your changes, keeping commits small and focused.
3. Add or update tests so that the package-level coverage does not decrease
   below its current watermark (currently ≥ 97%).
4. Run `make all` and verify it exits cleanly before opening a PR.

---

## Commit Messages

Follow the [Conventional Commits](https://www.conventionalcommits.org/en/v1.0.0/)
specification:

```
<type>(<scope>): <short summary>

[optional body]

[optional footer(s)]
```

| Type | When to use |
|------|-------------|
| `feat` | New feature |
| `fix` | Bug fix |
| `docs` | Documentation only |
| `test` | Adding or fixing tests |
| `refactor` | Code change that is neither a fix nor a feature |
| `perf` | Performance improvement |
| `chore` | Build, tooling, or CI changes |

Example:
```
feat(query): add LIMIT / OFFSET support to Q builder

Adds WithLimit(n) and WithOffset(n) methods to *QueryBuilder so callers
can page through large result sets without raw SQL.

Closes #42
```

---

## Pull Requests

- Target the `main` branch.
- Fill in the pull-request template completely.
- Link any related issues with `Closes #NNN` or `Fixes #NNN`.
- Keep PRs focused — one logical change per PR makes review faster.
- All CI checks must pass before a PR can be merged.
- At least one maintainer approval is required.

---

## Reporting Bugs

Use the **Bug report** issue template on GitHub. Please include:

- Go version (`go version`)
- Operating system and architecture
- Strata version or commit SHA
- Minimal reproducible example
- Observed behaviour vs expected behaviour

---

## Requesting Features

Use the **Feature request** issue template. Describe:

- The problem you are trying to solve
- Your proposed solution (if any)
- Any alternatives you considered

---

## Code Style

- Code is formatted with `gofmt` / `goimports` — the CI will reject unformatted diffs.
- Follow the conventions in [Effective Go](https://go.dev/doc/effective_go) and
  [Go Code Review Comments](https://github.com/golang/go/wiki/CodeReviewComments).
- Public API symbols must have Go doc comments.
- Prefer table-driven tests using `testify/require` and `testify/assert`.

---

## Testing

| Category | Command | Notes |
|----------|---------|-------|
| Unit / white-box | `go test -tags dev ./...` | No external services |
| Integration | `go test -tags integration ./...` | Requires Docker |
| Coverage | `make cover` | Generates `coverage.out` + HTML report |
| Benchmarks | `go test -bench=. -benchmem ./...` | |

New code paths must be covered by tests. Branches that are genuinely
unreachable (e.g. `io.ReadFull` on `/dev/urandom`) should have an explanatory
comment rather than a test that requires production-code changes.

---

## License

By contributing you agree that your contributions will be licensed under the
[Apache License 2.0](LICENSE) that covers the project.
