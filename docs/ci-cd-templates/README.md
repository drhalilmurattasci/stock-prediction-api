# CI/CD

The real workflows now live in [`.github/workflows/`](../../.github/workflows/):

- **`ci.yml`** — lint (ruff), format check, type-check (mypy), and tests (pytest) on push/PR.
- **`cd.yml`** — build and push the Docker image to GHCR on a published release.

## Pushing workflow files

GitHub blocks pushing files under `.github/workflows/` unless the credential has the
`workflow` OAuth scope. If a push is rejected for this reason, refresh your auth:

```bash
gh auth refresh -s workflow
# or use a Personal Access Token that includes the `workflow` scope
```

The previous `*.yml.example` placeholders have been promoted and removed.
