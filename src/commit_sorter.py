#!/usr/bin/python3
import argparse
import os
import subprocess
import tempfile

from simulator.util import validate_git_dir


def load_commits(git_dir):
    cmd = f"""git --git-dir {git_dir} log --pretty=format:"%H" """
    return subprocess.check_output(cmd, shell=True, text=True).splitlines()


# https://stackoverflow.com/questions/22714371/how-can-i-sort-a-set-of-git-commit-ids-in-topological-order
def order(commits, git_dir):
    if not commits:
        return commits

    with tempfile.NamedTemporaryFile(mode="w", delete=False, prefix="commit_", suffix=".txt") as tmp:
        tmp.write("\n".join(commits))
        tmp.write("\n")
        tmp.flush()

        cmd = f"""git --git-dir {git_dir} rev-list --date-order  $(cat {tmp.name}) | grep --file {tmp.name} --max-count $(wc -l < {tmp.name})"""
        out = subprocess.check_output(cmd, shell=True, text=True)
        result = out.splitlines()
        result.reverse()
        return result


class CommitOrderCli:

    def __init__(self, argv):
        parser = argparse.ArgumentParser(
            formatter_class=argparse.ArgumentDefaultsHelpFormatter,
            description='Returns an ordered list (from old to new) of commits')
        parser.add_argument("commits", nargs="+", help="The commits to order")
        parser.add_argument("-g", "--git-dir", metavar='git_dir', help="the directory containing the git repo", nargs=1,
                            default=[f"{os.getcwd()}/.git"])
        parser.add_argument("-d", "--debug", help="print the commits including timestamp", action='store_true')
        args = parser.parse_args(argv)
        git_dir = validate_git_dir(args.git_dir[0])
        commits = args.commits
        ordered = order(commits, git_dir)

        if args.debug:
            for commit in ordered:
                cmd = f"""git --git-dir {git_dir} show -s --format='%H | %ad | %ci' {commit}"""
                subprocess.run(cmd, shell=True, text=True)
        else:
            for commit in ordered:
                print(commit)
