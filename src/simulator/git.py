import subprocess
from simulator.log import error, info
from simulator.util import exit_with_error


def is_inside_git_repo():
    try:
        subprocess.run(['git', 'rev-parse', '--is-inside-work-tree'],
                       stdout=subprocess.PIPE,
                       stderr=subprocess.PIPE,
                       check=True,
                       text=True)
        return True
    except subprocess.CalledProcessError:
        return False


def is_git_installed():
    try:
        subprocess.run(['git', '--version'],
                       stdout=subprocess.PIPE,
                       stderr=subprocess.PIPE,
                       check=True,
                       text=True)
        return True
    except subprocess.CalledProcessError:
        return False


def get_last_commit_hash():
    try:
        result = subprocess.run(['git', 'rev-parse', 'HEAD'],
                                stdout=subprocess.PIPE,
                                text=True,
                                check=True)
        return result.stdout.strip()
    except subprocess.CalledProcessError:
        return None


def git_init():
    try:
        subprocess.run(['git', 'init', '-q'],
                       stdout=subprocess.PIPE,
                       text=True,
                       check=True)
    except subprocess.CalledProcessError:
        exit_with_error("Failed to create git repo")


def are_there_modified_files():
    try:
        result = subprocess.run(['git', 'status', '--porcelain'],
                                stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE,
                                check=True,
                                text=True)
        return bool(result.stdout.strip())
    except subprocess.CalledProcessError:
        return False


def commit_modified_files(commit_message):
    if not are_there_modified_files():
        info("No modified files found. Skipping 'git commit'.")
        return

    try:
        subprocess.run(['git', 'add', '-A'])
        subprocess.run(['git', 'commit', '-m', commit_message], check=True)
    except subprocess.CalledProcessError as e:
        error(f"Error: {e}")
