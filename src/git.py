from git import Repo
from git.exc import GitCommandError
import logging

PATH_OF_GIT_REPO = r'.git'  # make sure .git folder is properly configured
COMMIT_MESSAGE = 'Write data to repo'


def git_push():
    logging.info("Committing export of data to repo")
    repo = Repo(PATH_OF_GIT_REPO)
    repo.git.add(update=True)
    repo.index.commit(COMMIT_MESSAGE)
    origin = repo.remote(name='origin')
    origin.push()

# def git_push():
#     try:
#         logging.info("Committing export of data to repo")
#         repo = Repo(PATH_OF_GIT_REPO)
#         repo.git.add(update=True)
#         repo.index.commit(COMMIT_MESSAGE)
#         origin = repo.remote(name='origin')
#         origin.push()
#     except GitCommandError:
#         print('An error occurred while pushing the code')
