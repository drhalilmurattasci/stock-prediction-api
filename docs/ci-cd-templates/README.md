# CI/CD

The real workflows now live in [`.github/workflows/`](../../.github/workflows/):

- **`ci.yml`** — lint (ruff), format check, type-check (mypy), ordinary tests,
  and a separate fresh TimescaleDB/PostgreSQL integration gate on every push/PR.
  The ordinary pytest job remains skip-capable when live-service variables are
  absent; `live-postgres` explicitly opts in only the destructive Postgres
  module with generated credentials and no vendor secrets.
- **`cd.yml`** — build and push the Docker image to GHCR on a published release.

## Pushing workflow files

GitHub blocks pushing files under `.github/workflows/` unless the credential has the
`workflow` OAuth scope. If a push is rejected for this reason, refresh your auth:

```bash
gh auth refresh -s workflow
# or use a Personal Access Token that includes the `workflow` scope
```

The previous `*.yml.example` placeholders have been promoted and removed.
