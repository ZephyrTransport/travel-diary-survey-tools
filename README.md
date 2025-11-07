# travel-diary-survey-tools
For collaborating on Travel Diary Survey Tools

## Setup Instructions for Windows Users

### Installing UV

1. Open PowerShell and run:
```powershell
powershell -c "irm https://astral.sh/uv/install.ps1 | iex"
```

2. Restart your terminal to ensure UV is in your PATH

3. Verify the installation:
```powershell
uv --version
```

### Using UV

Create a virtual environment:
```powershell
uv sync
```

In VSCode you may need to restart terminal or select the interpreter manually. 