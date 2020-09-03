from git import Repo
from git.exc import GitCommandError
import logging

COMMIT_MESSAGE = "Write data to repo"


def git_push():
    try:
        logging.info("Committing export of data to repo")
        repo = Repo(".")
        repo.git.add(update=True)
        repo.index.commit(COMMIT_MESSAGE)
        origin = repo.remote(name="origin")
        current_branch = repo.active_branch.name
        origin.push(refspec=current_branch)
    except GitCommandError:
        print("An error occurred while pushing the code")
