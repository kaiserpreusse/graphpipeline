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


class SomeTestParserArguments(ReturnParser):
    def __init__(self):
        super(SomeTestParserArguments, self).__init__()

        self.source = NodeSet(['Source'], merge_keys=['source_id'])
        self.target = NodeSet(['Target'], merge_keys=['target_id'])
        self.rels = RelationshipSet('FOO', ['Source'], ['Target'], ['source_id'], ['target_id'])

        self.arguments = ['taxid']

    def run_with_mounted_arguments(self):
        self.run(self.taxid)

    def run(self, taxid):
        for i in range(100):
            self.source.add_node({'source_id': i})
            self.target.add_node({'target_id': i})
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


class TestParserSetSelection:
    def test_parserset_selection_by_parser_class(self):
        ps = ParserSet()
        p1 = SomeTestParser()
        p2 = RootTestParser()
        p3 = DependingTestParser()

        ps.add(p1)
        ps.add(p2)
        ps.add(p3)

        ps.select(parser=[SomeTestParser])

        assert len(ps.parsers) == 1
        assert len(ps._parser_stash) == 2

        assert p1 in ps.parsers
        assert p2 in ps._parser_stash
        assert p3 in ps._parser_stash

    def test_parserset_selection_by_parser_class_mulitple_times(self):
        # same parser class can exist in ParserSet multiple times (e.g. with different arguments)
        ps = ParserSet()
        p1 = SomeTestParserArguments()
        p1.taxid = '9606'
        other_p1 = SomeTestParserArguments()
        other_p1.taxid = '10090'
        p2 = RootTestParser()
        p3 = DependingTestParser()

        ps.add(p1)
        ps.add(other_p1)
        ps.add(p2)
        ps.add(p3)

        ps.select(parser=[SomeTestParserArguments])

        assert len(ps.parsers) == 2
        assert len(ps._parser_stash) == 2

        assert p1 in ps.parsers
        assert other_p1 in ps.parsers
        assert p2 in ps._parser_stash
        assert p3 in ps._parser_stash

    def test_parserset_selection_by_name(self):
        ps = ParserSet()
        p1 = SomeTestParser()
        p2 = RootTestParser()
        p3 = DependingTestParser()

        ps.add(p1)
        ps.add(p2)
        ps.add(p3)

        ps.select(parser=[p1.__class__.__name__])

        assert len(ps.parsers) == 1
        assert len(ps._parser_stash) == 2

        assert p1 in ps.parsers
        assert p2 in ps._parser_stash
        assert p3 in ps._parser_stash

    def test_parserset_selection_by_parser_class_multiple_parsers(self):
        ps = ParserSet()
        p1 = SomeTestParser()
        p2 = RootTestParser()
        p3 = DependingTestParser()

        ps.add(p1)
        ps.add(p2)
        ps.add(p3)

        ps.select(parser=[SomeTestParser, DependingTestParser])

        assert len(ps.parsers) == 2
        assert len(ps._parser_stash) == 1

        assert p1 in ps.parsers
        assert p2 in ps._parser_stash
        assert p3 in ps.parsers


class TestParserSetSerialize:

    def test_deserialize_whitelist(self, tmp_path):

        ps = ParserSet()
        p1 = SomeTestParser()
        p2 = RootTestParser()
        p3 = DependingTestParser()

        ps.add(p1)
        ps.add(p2)
        ps.add(p3)

        ps.serialize(tmp_path)

        reloaded_ps = ParserSet.deserialize(tmp_path, whitelist=[SomeTestParser])

        assert len(reloaded_ps.parsers) == 1
        assert p1.__class__.__name__ in [x.name for x in reloaded_ps.parsers]
        assert p2.__class__.__name__ not in [x.name for x in reloaded_ps.parsers]
        assert p3.__class__.__name__ not in [x.name for x in reloaded_ps.parsers]

    def test_deserialize_whitelist_same_parser_multiple_times(self, tmp_path):
        # same parser class can exist in ParserSet multiple times (e.g. with different arguments)
        ps = ParserSet()
        p1 = SomeTestParserArguments()
        p1.taxid = '9606'
        other_p1 = SomeTestParserArguments()
        other_p1.taxid = '10090'
        p2 = RootTestParser()
        p3 = DependingTestParser()

        ps.add(p1)
        ps.add(other_p1)
        ps.add(p2)
        ps.add(p3)

        ps.serialize(tmp_path)

        reloaded_ps = ParserSet.deserialize(tmp_path, whitelist=[SomeTestParserArguments])

        assert len(reloaded_ps.parsers) == 2
        assert p1.__class__.__name__ in [x.name for x in reloaded_ps.parsers]
        assert other_p1.__class__.__name__ in [x.name for x in reloaded_ps.parsers]
        assert p2.__class__.__name__ not in [x.name for x in reloaded_ps.parsers]
        assert p3.__class__.__name__ not in [x.name for x in reloaded_ps.parsers]

    def test_deserialize_whitelist_multiple_parsers(self, tmp_path):

        ps = ParserSet()
        p1 = SomeTestParser()
        p2 = RootTestParser()
        p3 = DependingTestParser()

        ps.add(p1)
        ps.add(p2)
        ps.add(p3)

        ps.serialize(tmp_path)

        reloaded_ps = ParserSet.deserialize(tmp_path, whitelist=[SomeTestParser, DependingTestParser])

        assert len(reloaded_ps.parsers) == 2
        assert p1.__class__.__name__ in [x.name for x in reloaded_ps.parsers]
        assert p3.__class__.__name__ in [x.name for x in reloaded_ps.parsers]
        assert p2.__class__.__name__ not in [x.name for x in reloaded_ps.parsers]
