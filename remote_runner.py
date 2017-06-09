import os
import logging

import docker
import paramiko
import time
from libcloud import DriverType
from libcloud import get_driver
from libcloud.compute.types import Provider as ProviderCompute

"""
TODO: Scheme out a way to have GPUs on standby. I.e. do the pull only and run the command
TODO: Add scheme for making sure that nodes get terminated and failed nodes get a notification sent out somewhere
        Probably some kind of design pattern will save me here. :)
"""

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_remote_runner(remote_runner=None):
    if not remote_runner:
        return None

    remote_runner_dict = {
        "EC2-GPU": RemoteRunnerEC2,
        "EC2": RemoteRunnerEC2,
        "debug": RemoteRunnerDebug,
        "local": RemoteRunnerLocal
    }

    return remote_runner_dict.get(remote_runner, 'local')


class RemoteRunnerBase(object):
    def __init__(self, job_args=None, owner=None, location=None, package=None, debug=False, run_id=None, inputs=None):
        self.job_args = job_args
        self.owner = owner
        self.location = location
        self.package = package

        # module to import should be user_code.<API_KEY>.Object
        self.module_to_import = self.generate_module()
        self.object_to_import = self.generate_object()
        self.debug = debug
        self.inputs = inputs
        self.run_id = run_id

    def _docker_image_to_run(self):
        package = self.location.split('.')[0]
        image = "%s/%s" % (self.owner, package)
        return image

    def generate_module(self):
        module_and_package = '.'.join(self.location.split('.')[:-1])
        return module_and_package

    def generate_object(self):
        # TODO: Replace with a regex
        return self.location.split('.')[-1]

    def _generate_cmd(self):
        cmd = "/usr/bin/python3 /run_object.py -p %s -u %s -r %s" % (self.location,
                                                                     self.owner,
                                                                     self.run_id)
        if len(self.inputs.keys()) == 0:
            return cmd

        cmd += " -i "

        for index, k in enumerate(self.inputs.keys()):
            v = self.inputs[k]
            cmd += v
            if index != len(self.inputs.keys()) -1:
                cmd += ' '
        return cmd

    def run(self):
        raise NotImplementedError


class RemoteRunnerEC2(RemoteRunnerBase):

    def run(self):
        # Spawn EC2 Instance
        if self.debug:
            print ("Debug network runner ran")
            return
        # spawn a new ec2/GPU instance with libcloud
        cls = get_driver(type=DriverType.COMPUTE, provider=ProviderCompute.EC2)
        IMAGE_ID = 'ami-6d95db0d'
        # https://aws.amazon.com/marketplace/fulfillment?productId=0ce7aca3-5b96-4ff4-8396-05245687380a&ref_=dtl_psb_continue&region=us-east-1
        SIZE_ID = 'g2.2xlarge'
        KEYPAIR_NAME = 'spawn-gpu'
        aws_access = os.environ.get('AWS_ACCESS', 'default_aws_access')
        aws_secret = os.environ.get('AWS_SECRET', 'default_aws_secret')
        driver = cls(key=aws_access,
                     secret=aws_secret,
                     region="us-west-1")
        sizes = driver.list_sizes()
        images = driver.list_images()

        size = [s for s in sizes if s.id == SIZE_ID][0]
        image = [i for i in images if i.id == IMAGE_ID][0]

        node = driver.create_node(name='test-node',
                                  image=image,
                                  size=size,
                                  ex_keyname=KEYPAIR_NAME)
        ip = driver.wait_until_running(nodes=[node])

        # connect to that new instance
        public_ip = ip[0][1][0]
        k = paramiko.RSAKey.from_private_key_file("/Users/apple/Downloads/spawn-gpu.pem")
        c = paramiko.SSHClient()
        c.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        wait_count = 10
        while True:
            try:
                c.connect(hostname=public_ip, username='ec2-user', pkey=k)
                break
            except Exception:
                time.sleep(10)
                wait_count -= 1
                if wait_count == 0:
                    raise Exception("Can't connect to Remote GPU")
                pass
        run_obj = self._generate_cmd()
        # docker run -it customer1/user1 /usr/bin/python3 /run_object.py -o ObjectA
        pull_docker = 'sudo docker pull emmjaykay/customer1.user'
        # execute the result
        cmds = [
            'sudo yum install docker -y',
            'sudo /etc/init.d/docker start',
            'sudo docker pull emmjaykay/customer1.user',
            'sudo docker run -it emmjaykay/customer1.user ' + run_obj
        ]
        for cmd in cmds:
            stdin, stdout, stderr = c.exec_command(cmd, get_pty=True)
            print(stdout.read())

        # collect result
        # Should be collected in to an S3 bucket of somekind somewhere

        # destroy node
        driver.destroy_node(node)

        return


class RemoteRunnerLocal(RemoteRunnerBase):

    def run(self):
        # Run Locally
        # Native runner
        # Need to do the equivalent of docker run -it customer1/user1 /usr/bin/python3 /run_object.py -o ObjectA
        client = docker.from_env()
        if isinstance(self.job_args, list) and len(self.job_args) > 0:
            self.job_args = self.job_args[0]

        cmd = self._generate_cmd()

        docker_image_to_run = self._docker_image_to_run()

        volumes = ['/data']
        volumes_bindings = {'/data/': {'bind': '/data/', 'mode': 'rw'} }
        # host_config = client.create_host_config(
        #     binds=volumes_bindings
        # )
        container = client.containers.run(docker_image_to_run, cmd,
                                          volumes=volumes_bindings,
                                          # volumes_from=volumes,
                                          # host_config=host_config,
                                          environment=self.job_args, detach=True)

        container.wait()

        outputs = container.logs()
        # Parse outputs and find the outputh path info

        # outputs set to a s3 bucket or where ever the outputs are set
        return outputs.decode("utf-8").rstrip("\n\r")


class RemoteRunnerDebug(RemoteRunnerBase):

    def run(self):
        # Do a Dry Run of inputs & outputs but don't run anything
        # For testing?
        logger.setLevel(logging.DEBUG)
        logger.debug("Running %s" % self.object_to_import)
        logger.setLevel(logging.INFO)


class RemoteRunner:
    def __init__(self, job_args=None, owner=None, location=None, package=None, debug=False, run_id=None, inputs=None):
        self.job_args = job_args
        self.owner = owner
        self.location = location
        self.package = package
        self.debug = debug
        self.remote_runner = None
        self.run_id = run_id
        self.inputs = inputs

        rr = RemoteRunnerLocal
        remote_runner = job_args.get('remote_runner', 'local')
        if remote_runner:
            rr = get_remote_runner(remote_runner=remote_runner)

        self.remote_runner = rr(job_args=job_args,
                                owner=self.owner,
                                location=self.location,
                                package=self.package,
                                debug=self.debug,
                                run_id=run_id,
                                inputs=self.inputs)

    def runner(self):
        return self.remote_runner.run()

    '''
    Architect's Notes
    Use SNS to register the docker container from within run_object.py

    Work manager that will use start_task_at_launch to pass in jobs.config file
        docs.aws.amazon.com/AmazonECS/latest/developerguide/start_task_at_launch.html
        container could read from a DB key for it's configs/job specification
    '''

if __name__ == "__main__":
    rrd = RemoteRunnerDebug(location="user1.ObjectB", package="Repo1")
    rrd.run()