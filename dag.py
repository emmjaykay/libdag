import argparse
import json
import logging
import os
import uuid

from node import node_factory
from tests.test_base import BaseTest

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def topological_sort(node_list=None):
    """
    :param node_list: a list of all Node objects that appear in the graph
    :return sorted_node_list: A topologically sorted list of all nodes that are in node_list, sorted by the requires
    field
    :return sorted_node_stages: A list of sets of nodes. Each set contains nodes that can be run in parallel

    Topological sort
    Step 0 - Initialize node_edge cache
    Step 1 - Find all nodes with no incoming edges. I.e., requires == [], and insert into a set node_no_incoming
    Step 2 - while node_no_incoming is not empty, remove 1 node named n
    Step 3 - Find all nodes that have n as an incoming edge, i.e., n appears in that nodes requires list
    Step 3.1 - Remove n's entry in the requires field and update node_edge cache
    Step 3.2 - If the candidate's requires field length is 0, add it to the node_no_incoming set. Also add it to
    new_no_node_incoming.
    Step 4 - Make note of children and add them to n's children list
    Step 5 Add new_no_node_incoming to sorted_node_stages
    """
    if node_list is None or len(node_list) == 0:
        return []

    # Step 0 - Instead of iterating over entire node's requires, we'll cache the number of edges here
    # Also keeps us from altering incoming list of nodes which will be reflected in Nodes
    node_edges = dict()
    for node in node_list:
        node_edges[node.name] = node_edges.get(node.name, 0) + len(node.requires)

    # Step 1
    sorted_node_list = []
    node_no_incoming = set([x for x in node_list if len(x.requires) == 0])

    sorted_node_stages = [set(node_no_incoming)]

    while len(node_no_incoming) != 0:
        # Step 2
        n = node_no_incoming.pop()

        sorted_node_list.append(n)
        nodes_with_n = []

        # Step 3
        for x in node_list:
            if n.name in x.requires:
                nodes_with_n.append(x)

        new_no_node_incoming = set()
        for candidate_node_to_remove in nodes_with_n:
            # Step 3.1 -- we use filter to get all entries out, checking for duplicates
            node_edge_to_remove = list(filter((n.name).__eq__, candidate_node_to_remove.requires))
            node_edges[candidate_node_to_remove.name] -= len(node_edge_to_remove)

            # Step 3.2 -- this is a new zero-incoming node
            if node_edges[candidate_node_to_remove.name] <= 0:
                new_no_node_incoming.add(candidate_node_to_remove)
                node_no_incoming.add(candidate_node_to_remove)

        for child_node in nodes_with_n:
            child_node.parents.append(n.name_id())
            n.children.append(child_node.name_id())

        # Step 4
        if len(new_no_node_incoming) != 0:
            sorted_node_stages.append(new_no_node_incoming)

    return sorted_node_list, sorted_node_stages


class Dag:
    def __init__(self, sorted_node_stages, job_args=None, debug=True, run_id=uuid.uuid4(), owner=None):
        self.sorted_node_stages = sorted_node_stages
        self.job_args = job_args
        self.debug = debug
        self.run_id = run_id
        self.owner = owner

        self.results_cache = self._create_load_results_cache()

    def _run_dir_path(self):
        s = '/data'
        s = os.path.join(s, self.owner)
        s = os.path.join(s, str(self.run_id))
        return s

    def _create_load_results_cache(self):
        run_path = self._run_dir_path()
        run_path_cache = os.path.join(run_path, "results.cache")

        results_cache = dict()
        if os.path.exists(run_path_cache):
            with open(run_path_cache, 'r') as results_cache_fd:
                results_cache = json.load(results_cache_fd)
        return results_cache

    def run(self):
        # Run the stages for each node here

        for stage in self.sorted_node_stages:
            for node in stage:
                inputs = {x: self.results_cache[x] for x in node.parents}
                node.inputs = inputs
                node.run(self.job_args)
                self.results_cache[node.name_id()] = node.outputs

        return self.results_cache


def dag_factory(js, owner=None, repo=None, debug=False, run_id=None):
    # add each node to a dictionary. We can have two of the same types of nodes
    # get called over and over again, so add them one at a time from the node
    # factory. The code will be the same but the inputs and outputs will be
    # different
    #
    node_list = []
    if run_id is None:
        run_id = uuid.uuid4()
    for obj in js['graph']:
        n = node_factory(js_obj=obj, owner=owner, repo=repo, debug=debug, run_id=run_id)
        node_list.append(n)

    node_list, node_stages = topological_sort(node_list)

    job_args = js

    dag = Dag(sorted_node_stages=node_stages, job_args=job_args, run_id=run_id, owner=owner)
    return dag


def main(args):

    if args.input_json:
        f = open(args.input_json)
        js = json.loads(f.read())
    else:
        bt = BaseTest()
        js = bt.js_obj
    dag = dag_factory(js, owner=args.user)

    dag.run()

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="scaffolding code for now")

    parser.add_argument('-i', "--input_json", help="input json DAG file")
    parser.add_argument('-u', "--user", help="user whose code we are to run")

    args = parser.parse_args()
    main(args)