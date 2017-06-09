import logging
import os
import tempfile
import uuid

from remote_runner import RemoteRunner

"""
      Which outputs from a Node goes to which output?
      We can't go by name alone since the same class might get called twice
TODO: Change All of the node.name references in graph with serial numbers so we can match the right class instances with
      the right inputs/outputs
"""

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def set_env_job_args(job_args):
    if job_args is None:
        return

    for key_pair in job_args:
        for env in key_pair.keys():
            env_var = key_pair[env]
            os.environ[env] = env_var


def unset_env_job_vars(job_args):
    if job_args is None:
        return

    for keypair in job_args:
        for env in keypair.keys():
            del os.environ[env]


class Node:
    def __init__(self, requires,
                 inputs,
                 outputs,
                 location="",
                 name="",
                 run_id=None,
                 node_id=uuid.uuid4(),
                 input_node=False,
                 output_node=False,
                 user=None,
                 repo=None,
                 debug=False):
        # these control either the locations or file descriptors of inputs and outputs
        self.inputs = inputs
        self.outputs = outputs

        # data processed here goes to the children in these lists
        self.requires = requires

        # this node takes input from outside the graph
        output_node = output_node
        # this node emits data, either a trained model or a transformed data set. I.e., a terminal node
        input_node = input_node

        # name of the package. Either a java package or a python module
        self.location = location

        self.children = []
        self.parents = []

        self.name = name
        self.id = node_id

        self.owner = user
        self.repo = repo

        self.debug = debug

        self.run_id = run_id

    def name_id(self):
        return "%s-%s" % (self.name, self.id)

    def run(self, job_args=None):
        logger.info("Running node: %s with input %s, args %s" % (self.name,
                                                                 self.inputs,
                                                                 job_args))

        if job_args is None:
            # TODO: Add a better default job_args
            job_args = {}

        rr = RemoteRunner(job_args=job_args,
                          owner=self.owner,
                          location=self.location,
                          package=self.repo,
                          debug=self.debug,
                          run_id=self.run_id,
                          inputs=self.inputs)

        outputs = rr.runner()

        logger.info(outputs)
        self.outputs = outputs

        return outputs

    def run_debug(self, job_args=None):
        logger.info("Running DEBUG Node: %s with input %s, args %s" % (self.name, self.inputs, job_args))

        return "outputs"

    def __str__(self):
        return "Node: %s, requires: %s" % (self.name, self.requires)


def inputs_outputs_factory(storage=None):
    if storage is '':
        storage = None

    d = {
        'flat_file': tempfile.TemporaryFile()
    }

    outputs = d.get(storage, tempfile.TemporaryFile())
    return outputs
    # output can be one of flat file, S3 bucket, GC bucket
    # only starting nodes will have inputs


def wc_login(user=None):
    if user != 'customer1' and user != 'customer3':
        raise Exception("No such user")
    return user


def node_factory(js_obj, owner=None, repo=None, debug=None, run_id=None):

    storage = js_obj.get('storage', 'flat_file')
    outputs = inputs_outputs_factory(storage)

    n = Node(
        requires=js_obj.get('requires', []),
        inputs=js_obj.get('inputs', None),
        outputs=outputs,
        location=js_obj['location'],
        name=js_obj['name'],
        run_id=run_id,
        input_node=js_obj.get("input", True),
        output_node=js_obj.get("output", True),
        user=owner,
        repo=repo,
        debug=debug
    )

    return n




