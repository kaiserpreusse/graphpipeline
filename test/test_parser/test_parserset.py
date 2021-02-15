import pytest

from graphpipeline.parser import ReturnParser,ParserSet
from graphio import NodeSet, RelationshipSet


@pytest.fixture
def TestParserClass():
    class TestParser(ReturnParser):
        def __init__(self, root_dir):
            super(TestParser, self).__init__(root_dir)

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

    return TestParser


def test_parserset_run(TestParserClass, tmp_path):
    some_parser = TestParserClass(tmp_path)
    ps = ParserSet()
    ps.parsers.append(some_parser)

    ps.run()

@pytest.mark.neo4j
def test_parserset_merge(clear_graph, TestParserClass, tmp_path, graph):
    """
    Use the TestParser, run_and_merge it, count graph elements. Merge again, count again.
    """
    some_parser = TestParserClass(tmp_path)
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
def test_dependency_parserset_merge(clear_graph, tmp_path, graph):
    """
    Test data loading functionality for two parsers that depend on each other.
    """

    class RootTestParser(ReturnParser):
        def __init__(self, root_dir):
            super(RootTestParser, self).__init__(root_dir)

            self.source = NodeSet(['Source'], merge_keys=['source_id'])
            self.target = NodeSet(['Target'], merge_keys=['target_id'])

        def run_with_mounted_arguments(self):
            self.run()

        def run(self):
            for i in range(100):
                self.source.add_node({'source_id': i})
                self.target.add_node({'target_id': i})

    class DependingTestParser(ReturnParser):

        def __init__(self, root_dir):
            super(DependingTestParser, self).__init__(root_dir)
            self.rels = RelationshipSet('FOO', ['Source'], ['Target'], ['source_id'], ['target_id'])

        def run_with_mounted_arguments(self):
            self.run()

        def run(self):
            for i in range(100):
                self.rels.add_relationship({'source_id': i}, {'target_id': i}, {'source': 'test'})

    root_parser = RootTestParser(tmp_path)
    depending_parser = DependingTestParser(tmp_path)

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