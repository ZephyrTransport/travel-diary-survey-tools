# GitHub Actions Workflows

This directory contains GitHub Actions workflows for continuous integration and deployment.

## Workflows

### CI Workflow (`ci.yml`)
Runs on push to main branches and on pull requests. Uses [uv](https://docs.astral.sh/uv/) for fast, reliable dependency management. Includes:
- **Linting**: Runs Ruff to check code style and formatting
- **Testing**: Runs pytest with coverage on multiple Python versions (3.11, 3.12, 3.13, 3.14)
- **Coverage**: Uploads coverage reports to Codecov (optional)

**Why UV?**
- ⚡ 10-100x faster than pip
- 🔒 Reproducible builds with `uv.lock`
- 💾 Automatic caching between runs
- 🎯 Minimal, only installs what's needed

### PR Checks Workflow (`pr-checks.yml`)
Additional checks that run on pull requests to ensure CI passes before merging.

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
     - `All checks passed`
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
uv run pytest --cov=travel_diary_survey_tools --cov-report=html
```

### Using pip (Alternative)
```bash
# Install in editable mode with dev dependencies
pip install -e ".[dev]"

# Run linting
ruff check .
ruff format --check .

# Run tests
pytest
```

### Running Linting Locally
```bash
# Check for issues
ruff check .

# Check formatting
ruff format --check .

# Auto-fix issues
ruff check --fix .

# Auto-format code
ruff format .
```

### Running Tests Locally
```bash
# Run all tests with coverage
pytest

# Run specific test file
pytest tests/test_linker.py

# Run with verbose output
pytest -v

# Run with coverage report
pytest --cov=travel_diary_survey_tools --cov-report=html
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
