import io
import unittest

from node import Node, node_factory, inputs_outputs_factory
from tests.test_base import BaseTest


class TestNode(BaseTest):
    def test_simple_node(self):
        n = Node(requires=['A'],
                 inputs=None,
                 outputs=None)
        assert n is not None

    def test_node_js_creation(self):
        for obj in self.js_obj['graph']:
            n = node_factory(obj, owner='customer1')
            assert n is not None

    def test_name_id_str(self):
        obj = self.js_obj['graph'][3]
        n = node_factory(obj, owner='customer1')

        name = n.name
        name_len = len(name)
        id_len = len(n.id.__str__())

        assert len(n.name_id()) == (name_len + id_len + 1)

        s = n.__str__()
        assert s == "Node: %s, requires: %s" % (n.name, n.requires)

    def test_input_creation(self):
        storage = 'flat_file'
        inputs = inputs_outputs_factory(storage)
        assert isinstance(inputs, io.BufferedRandom)

        storage = 'asdfasdf'
        inputs = inputs_outputs_factory(storage)
        assert isinstance(inputs, io.BufferedRandom)

        storage = ''
        inputs = inputs_outputs_factory(storage)
        assert isinstance(inputs, io.BufferedRandom)

    def test_run(self):
        obj = self.js_obj['graph'][3]
        n = node_factory(obj, owner='customer1', debug=True)
        job_args = {}
        job_args['remote_runner'] = 'debug'
        n.run(job_args=job_args)
