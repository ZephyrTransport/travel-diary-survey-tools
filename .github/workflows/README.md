# GitHub Actions Workflows

This directory contains GitHub Actions workflows for continuous integration and deployment.

## Workflows

### CI Workflow (`ci.yml`)
Runs on pushes to `main` and `develop`, and on pull requests into them. Uses [uv](https://docs.astral.sh/uv/) for fast, reliable dependency management. Jobs:
- **Lint**: Ruff lint and format check
- **Test (Python 3.11–3.14)**: `uv sync --group test`, then pytest with coverage on each version
- **Coverage**: uploads the coverage report to Codecov (optional; never fails the build)
- **Check Results**: passes only if Lint and every Test job passed; the one check to require
- **Generate Column Matrix**: regenerates `docs/COLUMN_REQUIREMENTS.md`. On a pull request it fails if the committed file is out of date; on a push it commits the regenerated file

**Why UV?**
- ⚡ 10-100x faster than pip
- 🔒 Reproducible builds with `uv.lock`
- 💾 Automatic caching between runs
- 🎯 Minimal, only installs what's needed

### Docs Workflow (`docs.yml`)
Builds the mkdocs site with `--strict` on pull requests into `develop` and `main`, so a broken cross-reference or missing page fails at review. Deploys it to GitHub Pages on pushes to `main`.

## Setting Up Branch Protection

To prevent merging PRs with failing tests or linting, configure branch protection rules:

1. Go to your repository on GitHub
2. Navigate to **Settings** > **Branches**
3. Click **Add branch protection rule**
4. Configure the following:
   - **Branch name pattern**: `main` (or your default branch)
   - ✅ **Require status checks to pass before merging**
   - Search for and select these required checks:
     - `Lint`
     - `Test (Python 3.11)`
     - `Test (Python 3.12)`
     - `Test (Python 3.13)`
     - `Test (Python 3.14)`
     - `Check Results`
     - `Generate Column Matrix`
   - ✅ **Require branches to be up to date before merging** (recommended)
   - ✅ **Require conversation resolution before merging** (optional)
   - ✅ **Include administrators** (optional, but recommended)
5. Click **Create** or **Save changes**

### Additional Recommended Settings

For enhanced protection:
- **Require a pull request before merging**: Enforce code review
- **Require approvals**: Set minimum number of reviewers (e.g., 1)
- **Dismiss stale pull request approvals when new commits are pushed**
- **Require review from Code Owners** (if using CODEOWNERS file)

## Local Development

### Using UV (Recommended)
```bash
# Install uv if you haven't already
# Windows (PowerShell)
powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"
# macOS/Linux
curl -LsSf https://astral.sh/uv/install.sh | sh

# Install all dependencies (including dev group)
uv sync --all-groups

# Run linting
uv run ruff check .
uv run ruff format --check .

# Run tests
uv run pytest

# Run tests with coverage
uv run pytest --cov=src --cov-report=html
```

The dev and test tools are uv dependency groups (`[dependency-groups]` in `pyproject.toml`), not pip extras, so `pip install -e ".[dev]"` does not install them. Use uv.

### Running Linting Locally
```bash
# Check for issues
uv run ruff check .

# Check formatting
uv run ruff format --check .

# Auto-fix issues
uv run ruff check --fix .

# Auto-format code
uv run ruff format .
```

### Running Tests Locally
```bash
# Run all tests with coverage
uv run pytest

# Run specific test file
uv run pytest tests/test_pipeline.py

# Run with verbose output
uv run pytest -v

# Run with coverage report
uv run pytest --cov=src --cov-report=html
```

## Codecov Integration (Optional)

To enable code coverage reporting:

1. Sign up at [codecov.io](https://codecov.io/)
2. Add your repository
3. Get your Codecov token
4. Add the token as a repository secret:
   - Go to **Settings** > **Secrets and variables** > **Actions**
   - Click **New repository secret**
   - Name: `CODECOV_TOKEN`
   - Value: Your Codecov token
5. Coverage reports will now be uploaded automatically

## Troubleshooting

### Workflow Not Running
- Ensure workflows are enabled in **Settings** > **Actions** > **General**
- Check that the branch names in the workflow match your repository structure

### Tests Failing in CI but Passing Locally
- Ensure all dependencies are properly specified in `pyproject.toml`
- Check for platform-specific issues (CI runs on Ubuntu)
- Verify Python version compatibility

### Ruff Errors
- Run `ruff check .` locally to see all issues
- Use `ruff check --fix .` to auto-fix many issues
- Check `pyproject.toml` for Ruff configuration
