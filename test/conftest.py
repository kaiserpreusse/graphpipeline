import pytest
import logging
from py2neo import Graph
from py2neo.wiring import WireError
from py2neo.client import ConnectionUnavailable

from time import sleep

logging.basicConfig()

log = logging.getLogger(__name__)

NEO4J_PASSWORD = 'test'

NEO4J_VERSIONS = [
    {'github_actions_host': 'neo4j35', 'version': '3.5', 'ports': (8474, 8473, 8687), 'uri_prefix': 'bolt'},
    {'github_actions_host': 'neo4j41', 'version': '4.1', 'ports': (9474, 9473, 9687), 'uri_prefix': 'bolt'},
    {'github_actions_host': 'neo4j42', 'version': '4.2', 'ports': (10474, 10473, 10687), 'uri_prefix': 'bolt'}
]

@pytest.fixture(scope='session')
def wait_for_neo4j():

    # check availability for both containers
    connected = False
    max_retries = 180
    retries = 0

    while not connected:
        try:
            # try to connect to both graphs, if it is not available `graph.run()` will
            # throw a ServiceUnavailable error
            for v in NEO4J_VERSIONS:
                # get Graph, bolt connection to localhost is default
                graph = Graph(password=NEO4J_PASSWORD, port=v['ports'][2], scheme='bolt')
                graph.run("MATCH (n) RETURN n LIMIT 1")
            connected = True

        except (ConnectionRefusedError, WireError, ConnectionResetError, ConnectionUnavailable):
            retries += 1
            log.warning(f"Connection unavailable on try {retries}. Try again in 1 second.")
            if retries > max_retries:
                break
            sleep(1)


@pytest.fixture(scope='session', params=NEO4J_VERSIONS)
def graph(request, wait_for_neo4j):
    yield Graph(host=request.param['github_actions_host'], password=NEO4J_PASSWORD, port=request.param['ports'][2], scheme='bolt')


@pytest.fixture
def clear_graph(graph):
    graph.run("MATCH (n) DETACH DELETE n")

    # remove indexes
    result = list(
        graph.run("CALL db.indexes()")
    )

    for row in result:
        # the result of the db.indexes() procedure is different for Neo4j 3.5 and 4
        # this should also be synced with differences in py2neo versions
        labels = []
        if 'tokenNames' in row:
            labels = row['tokenNames']
        elif 'labelsOrTypes' in row:
            labels = row['labelsOrTypes']

        properties = row['properties']

        # multiple labels possible?
        for label in labels:
            q = "DROP INDEX ON :{}({})".format(label, ', '.join(properties))
