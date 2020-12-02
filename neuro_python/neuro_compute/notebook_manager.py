"""
The notebook_manager module provides functions to interact with the
Notebook Manager in Neuroverse.
"""
import datetime
from retrying import retry
import subprocess

from neuro_python.neuro_call import neuro_call

def list_active_sessions():
    """
    Get list of active notebook sessions
    """
    
    return neuro_call("8080", "notebookmanagementservice", "GetDetailedSessionList", None)

last_session_delay_run=datetime.datetime(1970, 1, 1, 8, 0)
KeepSessionRunningResponse=''
@retry(wait_exponential_multiplier=1000, wait_exponential_max=10000,stop_max_attempt_number=7)
def delay_session_shutdown():
    """
    Delay a notebook session from being shutdown automatically
    """
    global last_session_delay_run
    global KeepSessionRunningResponse
    if datetime.datetime.utcnow()-last_session_delay_run>datetime.timedelta(minutes=5):
        last_session_delay_run=datetime.datetime.utcnow()
        KeepSessionRunningResponse=neuro_call("8080", "notebookmanagementservice", "KeepSessionRunning", None)
        return KeepSessionRunningResponse
    else:
        return KeepSessionRunningResponse

    
def clone_repo(repo_name,branch,remote_url,user_name,password,email):
    """
    Clone a branch from a remote git repository. The repo and branch need to be already created. 
    This code expects all notebooks in the remote repo to start with a ".". This function will automatically remove the "." prefix.
    """
    user=user_name
    password=password
    remote_url=remote_url.split("://")[-1].split("@")[-1]
    repo_name=repo_name
    branch=branch
    command = 'git clone --branch %s https://%s:%s@%s %s; '%(branch,user,password,remote_url,repo_name+'#'+branch)

    output = subprocess.Popen(command, shell=True, executable='/bin/bash', stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    output.wait()
    err=output.communicate()[1].decode('utf-8')
    if 'fatal:' in err:
        raise Exception(err)

    command = 'cd %s; git config user.email "%s"; '%(repo_name+'#'+branch,git_email)

    output = subprocess.Popen(command, shell=True, executable='/bin/bash', stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    output.wait()
    err=output.communicate()[1].decode('utf-8')
    if 'fatal:' in err:
        raise Exception(err)

    command = 'cd %s; git config user.name "%s"; '%(repo_name+'#'+branch,git_user)

    output = subprocess.Popen(command, shell=True, executable='/bin/bash', stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    output.wait()
    err=output.communicate()[1].decode('utf-8')
    if 'fatal:' in err:
        raise Exception(err)

    with open('%s/.gitignore'%(repo_name+'#'+branch),'w') as file:
        file.write('''
    *.ipynb
    !.*.ipynb
    .ipynb_checkpoints/
    ''')

    command = 'cd %s; '%(repo_name+'#'+branch)
    command += 'git add .gitignore; '
    command += 'git commit -m "init"; '
    command += 'push origin; '
    command += "rename 's/^.//' .*.ipynb; "
    command += "tmp=$PWD; for folder in $(find -type d -print); do cd \"$tmp/${folder:2}\"; rename 's/^.//' .*.ipynb; done; cd $tmp"

    output = subprocess.Popen(command, shell=True, executable='/bin/bash', stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    output.wait()
    err=output.communicate()[1].decode('utf-8')
    if 'fatal:' in err:
        raise Exception(err)
