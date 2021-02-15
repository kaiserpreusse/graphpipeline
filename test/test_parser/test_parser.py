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


@pytest.fixture
def test_parser_with_data(TestParserClass, tmp_path):
    p = TestParserClass(tmp_path)
    p.run()
    return p


def test_parser_container(test_parser_with_data):
    for nodeset in test_parser_with_data.container.nodesets:
        assert isinstance(nodeset, NodeSet)

    for relset in test_parser_with_data.container.relationshipsets:
        assert isinstance(relset, RelationshipSet)
