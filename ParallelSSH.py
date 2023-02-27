import subprocess
from datetime import datetime,timedelta

class ParallelSSH:

    SSH_BIN="/usr/bin/ssh"

    def __init__(self,max_procs=1,timeout=None,hostlist=None):
        self.__check_ssh()
        self.max_procs=max_procs
        self.timeout=timeout
        self.cmd_output=None
        self.cmd_failures=None


    def __append_failure(self,hostname,error):
        self.cmd_failures.append((hostname,error))

    def __append_output(self, hostname, stdout,stderr):
        self.cmd_output.append((hostname,stdout.decode("UTF-8"),stderr.decode("UTF-8")))


    def __check_ssh(self):

        """Check that available SSH implementation is compatible. Raise error if not"""
        version_cmd=[self.SSH_BIN, "-V"]
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


    def get_output(self):
        return self.cmd_output

    def get_failures(self):
        return self.cmd_failures

    def run_cmd(self,remote_cmd):

        self.cmd_output=[]
        self.cmd_failures=[]

        #base SSH command
        ssh_cmd=[self.SSH_BIN, "-nqo", "BatchMode=yes"]
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
                        self.__append_output(get_hostname(proc),out,err)
                        running_procs[i]=None
                        continue
                    if not self.timeout==None:
                        #check if process has exceeded timeout
                        if datetime.now() >= start_time+timedelta(seconds=self.timeout):
                            proc.kill()
                            running_procs[i]=None
                            self.__append_failure(get_hostname(proc),"Timeout Exceeded")
            #remove finished processes
            running_procs=[x for x in running_procs if x is not None]

    def set_hostlist(self,hostlist):
        self.hostlist=hostlist

    def set_max_procs(self, max_procs):
        self.max_procs=max_procs

    def set_timeout(self,timeout):
        self.timeout=timeout

