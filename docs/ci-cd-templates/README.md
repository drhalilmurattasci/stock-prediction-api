# CI/CD templates (PLACEHOLDER)

These `*.yml.example` files are GitHub Actions placeholders. They live here instead of
`.github/workflows/` because the token used for the initial push lacked the `workflow`
OAuth scope (GitHub blocks pushing workflow files without it).

**To activate:** re-authenticate with `gh auth refresh -s workflow` (or use a PAT that
has the `workflow` scope), then move each file into `.github/workflows/` with its real
name (drop the `.example` suffix) and implement the steps.
