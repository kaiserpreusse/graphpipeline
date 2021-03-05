import pytest

from graphpipeline.parser import ReturnParser,ParserSet
from graphio import NodeSet, RelationshipSet



class SomeTestParser(ReturnParser):
    def __init__(self):
        super(SomeTestParser, self).__init__()

        self.source = NodeSet(['Source'], merge_keys=['source_id'])
        self.target = NodeSet(['Target'], merge_keys=['target_id'])
        self.rels = RelationshipSet('FOO', ['Source'], ['Target'], ['source_id'], ['target_id'])

    def run_with_mounted_arguments(self):
        self.run()

    def run(self):
        for i in range(100):
            self.source.add_node({'source_id': i})
            self.target.add_node({'target_id': i})
            self.rels.add_relationship({'source_id': i}, {'target_id': i}, {'source': 'test'})


class RootTestParser(ReturnParser):
    def __init__(self):
        super(RootTestParser, self).__init__()

        self.source = NodeSet(['Source'], merge_keys=['source_id'])
        self.target = NodeSet(['Target'], merge_keys=['target_id'])

    def run_with_mounted_arguments(self):
        self.run()

    def run(self):
        for i in range(100):
            self.source.add_node({'source_id': i})
            self.target.add_node({'target_id': i})

class DependingTestParser(ReturnParser):

    def __init__(self):
        super(DependingTestParser, self).__init__()
        self.rels = RelationshipSet('FOO', ['Source'], ['Target'], ['source_id'], ['target_id'])

    def run_with_mounted_arguments(self):
        self.run()

    def run(self):
        for i in range(100):
            self.rels.add_relationship({'source_id': i}, {'target_id': i}, {'source': 'test'})


@pytest.mark.neo4j
def test_parserset_merge(clear_graph, tmp_path, graph):
    """
    Use the TestParser, run_and_merge it, count graph elements. Merge again, count again.
    """
    some_parser = SomeTestParser()
    ps = ParserSet()
    ps.parsers.append(some_parser)

    ps.run_and_merge(graph)

    result = graph.run("MATCH (s:Source) RETURN count(distinct s) AS count").data()
    assert result[0]['count'] == len(some_parser.source.nodes)

    result = graph.run("MATCH (t:Target) RETURN count(distinct t) AS count").data()
    assert result[0]['count'] == len(some_parser.target.nodes)

    result = graph.run("MATCH (s:Source)-[r:FOO]->(t:Target) RETURN count(distinct r) AS count").data()
    assert result[0]['count'] == len(some_parser.rels.relationships)

    ps.run_and_merge(graph)

    result = graph.run("MATCH (s:Source) RETURN count(distinct s) AS count").data()
    assert result[0]['count'] == len(some_parser.source.nodes)

    result = graph.run("MATCH (t:Target) RETURN count(distinct t) AS count").data()
    assert result[0]['count'] == len(some_parser.target.nodes)

    result = graph.run("MATCH (s:Source)-[r:FOO]->(t:Target) RETURN count(distinct r) AS count").data()
    assert result[0]['count'] == len(some_parser.rels.relationships)


@pytest.mark.neo4j
def test_parserset_merge_sequential(clear_graph, tmp_path, graph):
    """
    Run the merge_sequential function on a ParserSet and assert that all data is in the database.
    """
    root_parser = RootTestParser()
    depending_parser = DependingTestParser()

    ps = ParserSet()
    ps.parsers.append(root_parser)
    ps.parsers.append(depending_parser)

    ps.run_and_merge_sequential(graph)
    # run_and_merge_sequential resets the parser after running
    # run again to get the data for asserts
    ps.run_with_mounted_arguments()
    result = graph.run("MATCH (s:Source) RETURN count(distinct s) AS count").data()
    assert result[0]['count'] == len(root_parser.source.nodes)

    result = graph.run("MATCH (t:Target) RETURN count(distinct t) AS count").data()
    assert result[0]['count'] == len(root_parser.target.nodes)

    result = graph.run("MATCH (s:Source)-[r:FOO]->(t:Target) RETURN count(distinct r) AS count").data()
    assert result[0]['count'] == len(depending_parser.rels.relationships)

    ps.run_and_merge_sequential(graph)
    # run_and_merge_sequential resets the parser after running
    # run again to get the data for asserts
    ps.run_with_mounted_arguments()
    result = graph.run("MATCH (s:Source) RETURN count(distinct s) AS count").data()
    assert result[0]['count'] == len(root_parser.source.nodes)

    result = graph.run("MATCH (t:Target) RETURN count(distinct t) AS count").data()
    assert result[0]['count'] == len(root_parser.target.nodes)

    result = graph.run("MATCH (s:Source)-[r:FOO]->(t:Target) RETURN count(distinct r) AS count").data()
    assert result[0]['count'] == len(depending_parser.rels.relationships)



@pytest.mark.neo4j
def test_parserset_create(clear_graph, tmp_path, graph):
    """
    Use the TestParser, run_and_create it, count graph elements. Merge again, count again.

    Only testing number of nodes in this most basic test. The number of relationships depends on the
    properties chosen to identify source/target.
    """

    some_parser = SomeTestParser()
    ps = ParserSet()
    ps.parsers.append(some_parser)

    ps.run_and_create(graph)

    result = graph.run("MATCH (s:Source) RETURN count(distinct s) AS count").data()
    assert result[0]['count'] == len(some_parser.source.nodes)

    result = graph.run("MATCH (t:Target) RETURN count(distinct t) AS count").data()
    assert result[0]['count'] == len(some_parser.target.nodes)

    ps.run_and_create(graph)

    result = graph.run("MATCH (s:Source) RETURN count(distinct s) AS count").data()
    assert result[0]['count'] == 2*len(some_parser.source.nodes)

    result = graph.run("MATCH (t:Target) RETURN count(distinct t) AS count").data()
    assert result[0]['count'] == 2*len(some_parser.target.nodes)


@pytest.mark.neo4j
def test_dependency_parserset_merge(clear_graph, graph):
    """
    Test data loading functionality for two parsers that depend on each other.
    """
    root_parser = RootTestParser()
    depending_parser = DependingTestParser()

    ps = ParserSet()
    ps.parsers.append(root_parser)
    ps.parsers.append(depending_parser)

    ps.run_and_merge(graph)

    result = graph.run("MATCH (s:Source) RETURN count(distinct s) AS count").data()
    assert result[0]['count'] == len(root_parser.source.nodes)

    result = graph.run("MATCH (t:Target) RETURN count(distinct t) AS count").data()
    assert result[0]['count'] == len(root_parser.target.nodes)

    result = graph.run("MATCH (s:Source)-[r:FOO]->(t:Target) RETURN count(distinct r) AS count").data()
    assert result[0]['count'] == len(depending_parser.rels.relationships)

    ps.run_and_merge(graph)

    result = graph.run("MATCH (s:Source) RETURN count(distinct s) AS count").data()
    assert result[0]['count'] == len(root_parser.source.nodes)

    result = graph.run("MATCH (t:Target) RETURN count(distinct t) AS count").data()
    assert result[0]['count'] == len(root_parser.target.nodes)

    result = graph.run("MATCH (s:Source)-[r:FOO]->(t:Target) RETURN count(distinct r) AS count").data()
    assert result[0]['count'] == len(depending_parser.rels.relationships)