import os
import subprocess
from pathlib import Path
import tomllib
from datetime import datetime

os.system(r'net use M: \\models.ad.mtc.ca.gov\data\models')
os.system(r'net use X: \\model3-a\Model3A-Share')

# Get the directory where this script is located
script_dir = Path(__file__).parent

# Find all Python files in the current directory that contain "pipeline" in their name
pipeline_files = os.listdir(script_dir)
for file in pipeline_files:
    # If doesn't start with 2 digits, or doesn't end with .py, remove it from the list
    if not (file.endswith('.py') and len(file) >= 6 and file[:2].isdigit()):
        pipeline_files.remove(file)

print(f"Found {len(pipeline_files)} pipeline files:")
for file in pipeline_files:
    print(f"  - {file}")

# Run each pipeline file with the config_path argument
config_path = str(script_dir.parent / 'config' / 'pipeline_config_test.toml')

# Load config

class PipelineLog:
    entries: list[tuple[int, str, str]] = []
    def __init__(self, config_path: Path):

        # Load config
        with open(config_path, "rb") as f:
            config = tomllib.load(f)
        self.log_path = Path(config['model']['dir']) / 'pipeline.log'

        # Populate class state
        if self.log_path.exists():
            with open(self.log_path, 'r') as log_file:
                for i, line in enumerate(log_file):
                    step, timestamp = line.strip().split(': ', 1)
                    self.entries.append((i, step, timestamp))
        else:
            self.log_path.touch()

    def is_up_to_date(self, step: str) -> bool:
        # If current step is newer than last step, return False
        print(f"Checking if step '{step}' is up to date...")
        for i, s, timestamp in self.entries:

            # to datetime
            timestamp = datetime.fromisoformat(timestamp)
            last_timestamp = datetime.fromisoformat(self.entries[i-1][2]) if i > 0 else datetime.min

            if (
                (s == step) and
                (
                    (i == 0) or
                    (i > 0 and last_timestamp < timestamp)
                )
            ):
                return True
        return False


    def update(self, step: str):
        timestamp = datetime.now().isoformat()
        self.entries.append((len(self.entries), step, timestamp))
        with open(self.log_path, 'a') as log_file:
            log_file.write(f"{step}: {timestamp}\n")

metalog = PipelineLog(config_path)

for file in pipeline_files:
    print(f"\n{'='*60}")
    print(f"Running {file}...")
    print(f"{'='*60}")
    step = Path(file).stem

    if metalog.is_up_to_date(step):
        print(f"{file} is up to date. Skipping...")
        continue

    subprocess.run(
            [os.sys.executable, str(script_dir / file), config_path],
            check=True
    )

    # Append log file with current step and timestamp
    metalog.update(step)
