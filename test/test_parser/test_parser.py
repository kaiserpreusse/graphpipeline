from graphpipeline.parser import ReturnParser
from graphio import NodeSet, RelationshipSet


def test_parser_container(tmp_path):
    class TestParser(ReturnParser):
        def __init__(self, root_dir):
            super(TestParser, self).__init__(root_dir)

            self.mynodes = NodeSet(['Test'], merge_keys=['uuid'])
            self.myrels = RelationshipSet('FOO', ['Test'], ['Target'], ['uuid'], ['target_id'])

    p = TestParser(tmp_path)

    p.mynodes.add_node({'uuid': 'abc123'})
    p.myrels.add_relationship({'uuid': 'abc123'}, {'target_id': '12345'}, {'source': 'test'})

    assert len(list(p.container.nodesets)) == 1
    assert len((p.container.relationshipsets)) == 1

    for nodeset in p.container.nodesets:
        assert isinstance(nodeset, NodeSet)

    for relset in p.container.relationshipsets:
        assert isinstance(relset, RelationshipSet)
