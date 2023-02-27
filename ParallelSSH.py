import subprocess
from datetime import datetime,timedelta
from os import path
"""
Class to run shell commands via SSH against multiple systems in parallel

Depends on OpenSSH


Example usage:

my_hostlist=['10.0.5.35','10.0.5.36', "10.0.6.15", "10.0.6.16"]
p_ssh = ParallelSSH.ParallelSSH(max_procs=5,timeout=5,hostlist=my_hostlist)
p_ssh.run_cmd("uptime")
uptime_success=p_ssh.get_successes()
p_ssh.set_timeout(15)
p_ssh.run_cmd("ps aux")
ps_successes=p_ssh.get_successes()
ps_failures=p_ssh.get_failures()
"""
class ParallelSSH:

    def __init__(self,max_procs=1,timeout=None,hostlist=None,ssh_bin="/usr/bin/ssh"):
        """constructor
        Args:
            + max_procs (int): maximum number of SSH process to run in parallel
            + timeout (int): time in seconds to wait before killing SSH processes
                that have not completed. If None, then waits until SSH process exits.
            + hostlist (list): list of strings containing hostnames or IP addresses that 
                commands will be run against
            + ssh_bin (string): full path to OpenSSH binary. Defaults to /usr/bin/ssh
        """
        self.ssh_bin=ssh_bin
        self.__check_ssh()
        self.max_procs=max_procs
        self.timeout=timeout
        self.hostlist=hostlist
        self.cmd_successes=None
        self.cmd_failures=None


    def __append_failure(self,hostname,error,returncode):
        self.cmd_failures.append((hostname,error,returncode))

    def __append_success(self, hostname, stdout,stderr,returncode):
        self.cmd_successes.append((hostname,stdout.decode("UTF-8"),stderr.decode("UTF-8"),returncode))


    def __check_ssh(self):
        """Check that available SSH implementation is compatible. Raise error if not"""
        if not path.isfile(self.ssh_bin):
            raise RuntimeError(f"Expected SSH path is missing: {self.ssh_bin}")
        version_cmd=[self.ssh_bin, "-V"]
        version_cmd_fmt=" ".join(version_cmd)
        #openssh prints version to stderr
        proc=subprocess.run(version_cmd, stdout=subprocess.PIPE,stderr=subprocess.STDOUT)
        if not proc.returncode == 0:
            raise RuntimeError(f"{version_cmd_fmt} returned non-zero exit code")
        ssh_ver=proc.stdout.decode("UTF-8")
        #print(ssh_ver)
        if len(ssh_ver) < 1:
            raise RuntimeError(f"No output from {version_cmd_fmt}")
        if not "openssh" in ssh_ver.lower():
            raise RuntimeError(f"Expecting OpenSSH implementation:\n {version_cmd_fmt} reported {ssh_ver}.")


    def get_successes(self):
        """ return results for hosts where command ran succesfully.
        Structure is list of tuples as:
            (<hostname>,<stdout output from host>,<stderr output from host>,<exit code>)
        """
        return self.cmd_successes

    def get_failures(self):
        """ return results for hosts where command failed. See __append_failure for data structure
        Structure is list of tuples:
            (<hostname>,<error message>,<exit code (None if unavaialable)>)
        """
        return self.cmd_failures

    def run_cmd(self,remote_cmd,expected_exit_code=0):
        """Run a command against the list of hosts
        Args:
            +remote_cmd (str): the command to run
            +expected_exit_code (int): Optional. Specify the return code you expect for the SSH process. Default is 0.
        """

        self.cmd_successes=[]
        self.cmd_failures=[]

        #base SSH command
        ssh_cmd=[self.ssh_bin, "-nqo", "BatchMode=yes"]
        cmds_to_run=[ ssh_cmd + [host,remote_cmd] for host in self.hostlist ]

        #hostname expected to be 4th argument in ssh command
        def get_hostname(process):
            return(process.args[3])

        running_procs=[]

        while True:
            #if there are processes waiting and free spots, add them
            while len(running_procs) < self.max_procs and len(cmds_to_run) > 0: 
                #start process and append to running procs as (proc, start_time)
                running_procs.append([subprocess.Popen(cmds_to_run.pop(), stdout=subprocess.PIPE, stderr=subprocess.PIPE), datetime.now()])


            #if no procs left and no cmds left to run, finished
            if len(running_procs)==0 and len(cmds_to_run)==0:
                break

            #iterate over processes until one or more finishes
            while not None in running_procs:
                for i in range(0,len(running_procs)):
                    proc,start_time = running_procs[i]
                    if not proc.poll()==None: #process finished
                        out,err=proc.communicate()
                        if proc.returncode==expected_exit_code:
                            self.__append_success(get_hostname(proc),out,err,proc.returncode)
                        else:
                            self.__append_failure(get_hostname(proc),"Unexpected Return Code", proc.returncode)
                        running_procs[i]=None
                    if not self.timeout==None:
                        #check if process has exceeded timeout
                        if datetime.now() >= start_time+timedelta(seconds=self.timeout):
                            proc.kill()
                            running_procs[i]=None
                            self.__append_failure(get_hostname(proc),"Timeout Exceeded",None)
            #remove finished processes
            running_procs=[x for x in running_procs if x is not None]

    def set_hostlist(self,hostlist):
        self.hostlist=hostlist

    def set_max_procs(self, max_procs):
        self.max_procs=max_procs

    def set_timeout(self,timeout):
        self.timeout=timeout

