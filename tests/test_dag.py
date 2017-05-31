import io

from dag import topological_sort, dag_factory
from node import node_factory
from tests.test_base import BaseTest


class BaseDagTest(BaseTest):
    def __init__(self, *args, **kwargs):
        super(BaseDagTest, self).__init__(*args, **kwargs)
        self.node_list = []
        for obj in self.js_obj['graph']:
            n = node_factory(js_obj=obj, owner='customer1')
            self.node_list.append(n)

        self.node_list, self.node_stages = topological_sort(self.node_list)


class DagTest(BaseDagTest):
    def test_dag_list(self):
        assert self.node_list[0].name == "ObjectA"
        assert self.node_list[1].name == "ObjectC" or \
               self.node_list[2].name == "ObjectC"
        assert self.node_list[1].name == "ObjectB" or \
               self.node_list[2].name == "ObjectB"
        assert self.node_list[3].name == "ObjectD"

        assert self.node_list[0].requires == []
        assert self.node_list[1].requires == ['ObjectA']
        assert self.node_list[2].requires == ['ObjectA']
        assert self.node_list[3].requires == ['ObjectC', 'ObjectB']

    def test_dag_stages(self):
        assert list(self.node_stages[0])[0].name == 'ObjectA'
        assert list(self.node_stages[1])[0].name == 'ObjectC' or \
               list(self.node_stages[1])[1].name == 'ObjectC'
        assert list(self.node_stages[1])[0].name == 'ObjectB' or \
               list(self.node_stages[1])[1].name == 'ObjectB'
        assert list(self.node_stages[2])[0].name == 'ObjectD'

    def test_dag_factory(self):
        dag = dag_factory(self.js_obj, owner='customer3', repo='user1', debug=True)

        assert list(dag.sorted_node_stages[0])[0].name == 'ObjectA'
        assert list(dag.sorted_node_stages[1])[0].name == 'ObjectC' or \
               list(dag.sorted_node_stages[1])[1].name == 'ObjectC'
        assert list(dag.sorted_node_stages[1])[0].name == 'ObjectB' or \
               list(dag.sorted_node_stages[1])[1].name == 'ObjectB'
        assert list(dag.sorted_node_stages[2])[0].name == 'ObjectD'

        cache = dag.run()

        assert len(cache) == 4

        for k in cache:
            v = cache[k]
            assert isinstance(v, io.BufferedRandom)

    def test_empty_list_dag(self):
        l = topological_sort([])
        assert l == []

        l = topological_sort(None)
        assert l == []

    def test_dag_vars(self):
        dag = dag_factory(self.js_obj, owner='customer1')
        job_args = [
            {'AWS_ACCESS_KEY': 'ACCESS_KEY', 'AWS_SECRET_KEY': 'SECRET_KEY'}
        ]
        assert dag.job_args['vars'] == job_args

