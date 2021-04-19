import pytest
import os

from graphpipeline.parser import ReturnParser, ParserSet, Parser
from graphpipeline.parser.parser import YieldParser
from graphio import NodeSet, RelationshipSet


class SomeParser(ReturnParser):
    def __init__(self):
        super(SomeParser, self).__init__()

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


@pytest.fixture
def test_parser_with_data(TestParserClass, tmp_path):
    p = TestParserClass(tmp_path)
    p.run()
    return p


class TestSerialization:

    def test__serialization_dir_name(self):
        p = ReturnParser()
        p.arguments = ['foo', 'bar']
        p.foo = 'test'
        p.bar = 'other_test'

        assert p._serialization_dir_name() == 'ReturnParser_foo_test_bar_other_test'

    def test_serialize(self, tmp_path):
        tp = SomeParser()
        tp.run()
        tp.serialize(str(tmp_path))

        parser_path = os.path.join(tmp_path, tp.__class__.__name__)

        assert os.path.exists(parser_path)

        for ns in tp.container.nodesets:
            ns_file_path = os.path.join(parser_path, ns.object_file_name(suffix='.json'))
            assert os.path.exists(ns_file_path)

        for rs in tp.container.nodesets:
            rs_file_path = os.path.join(parser_path, rs.object_file_name(suffix='.json'))
            assert os.path.exists(rs_file_path)

    def test_deserialize(self, tmp_path):
        tp = SomeParser()
        tp.run()
        tp.serialize(str(tmp_path))

        output_path = os.path.join(str(tmp_path), tp.name)
        reloaded_tp = Parser.deserialize(output_path)

        for ns in reloaded_tp.container.nodesets:
            assert isinstance(ns, NodeSet)
        for ns in reloaded_tp.container.relationshipsets:
            assert isinstance(ns, RelationshipSet)

        assert len(tp.container.nodesets) == len(reloaded_tp.container.nodesets)
        assert len(tp.container.relationshipsets) == len(reloaded_tp.container.relationshipsets)


def test_parser_arguments():
    p = ReturnParser()
    p.arguments = ['foo', 'bar']
    p.foo = 'test'
    p.bar = 'other_test'

    assert p.get_arguments()['foo'] == 'test'
    assert p.get_arguments()['bar'] == 'other_test'


def test_parser_container(test_parser_with_data):
    for nodeset in test_parser_with_data.container.nodesets:
        assert isinstance(nodeset, NodeSet)

    for relset in test_parser_with_data.container.relationshipsets:
        assert isinstance(relset, RelationshipSet)



class TestYieldParser:

    @pytest.fixture(scope='class')
    def SimpleTestYieldParser(self):
        class SimpleTestYieldParser(YieldParser):

            def __init__(self):
                super(SimpleTestYieldParser, self).__init__()

                self.source = NodeSet(['YieldSource'], merge_keys=['uid'])

            def run_with_mounted_arguments(self):
                self.run()

            def run(self):
                self.source.nodes = self.yield_node_function()

            def yield_node_function(self):
                for i in range(100):
                    yield {'uid': i}

        return SimpleTestYieldParser

    @pytest.mark.neo4j
    def test_simple_yield_parser_merge(self, clear_graph, graph, tmp_path, SimpleTestYieldParser):


        yield_parser = SimpleTestYieldParser()
        yield_parser.run_with_mounted_arguments()

        yield_parser.merge(graph)

        result = graph.run("MATCH (n:YieldSource) RETURN count(distinct n) AS count").data()
        assert result[0]['count'] == 100

        yield_parser.merge(graph)

        result = graph.run("MATCH (n:YieldSource) RETURN count(distinct n) AS count").data()
        assert result[0]['count'] == 100

    @pytest.mark.neo4j
    def test_simple_yield_parser_create(self, clear_graph, graph, tmp_path, SimpleTestYieldParser):
        yield_parser = SimpleTestYieldParser()
        yield_parser.run_with_mounted_arguments()

        yield_parser.create(graph)

        result = graph.run("MATCH (n:YieldSource) RETURN count(distinct n) AS count").data()
        assert result[0]['count'] == 100

        yield_parser.run_with_mounted_arguments()
        yield_parser.create(graph)

        result = graph.run("MATCH (n:YieldSource) RETURN count(distinct n) AS count").data()
        assert result[0]['count'] == 200