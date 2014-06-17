import requests

def get_distribution_branch(branch):
    """Checks (and stores) target branch (master/lts)
    """
    if branch.startswith('pull/'):
        # get pull request number (pull/#NUMBER/merge)
        pr_number = branch.split('/')[1]
        url = "https://api.github.com/repos/reverbrain/elliptics/pulls/{0}".format(pr_number)
        r = requests.get(url)
        pr_info = r.json()
        distribution_branch = pr_info["base"]["ref"]
    else:
        distribution_branch = branch

    if distribution_branch == "master":
        return "testing"
    elif distribution_branch == "lts":
        return "stable"
    else:
        raise RuntimeError("Wrong branch was specified: {0}".format(branch))
