from py2neo import Graph


class ParserSet:
    """
    A container for a set of Parser objects.

    Used to run batch operations: Merge all, export all to CSV etc.
    """

    def __init__(self):
        self.parsers = []

    def run_with_mounted_arguments(self):
        """
        Run all parsers with mounted arguments.
        """
        for p in self.parsers:
            p.run_with_mounted_arguments()

    def merge(self, graph):
        """
        Fist merge all NodeSets, then merge all RelationshipSets in the ParserSet.
        """
        self.merge_nodes(graph)
        self.merge_relationships(graph)

    def merge_relationships(self, graph):
        for p in self.parsers:
            for relset in p.container.relationshipsets:
                relset.create_index(graph)
                relset.merge(graph)

    def merge_nodes(self, graph):
        for p in self.parsers:
            for nodeset in p.container.nodesets:
                nodeset.create_index(graph)
                nodeset.merge(graph)

    def create(self, graph):
        """
        Fist merge all NodeSets, then merge all RelationshipSets in the ParserSet.
        """
        self.create_nodes(graph)
        self.create_relationships(graph)

    def create_relationships(self, graph):
        for p in self.parsers:
            for relset in p.container.relationshipsets:
                relset.create_index(graph)
                relset.create(graph)

    def create_nodes(self, graph):
        for p in self.parsers:
            for nodeset in p.container.nodesets:
                nodeset.create_index(graph)
                nodeset.create(graph)

    def _reset(self):
        for p in self.parsers:
            p._reset_parser()

    def run_and_merge_sequential(self, graph: Graph):
        """
        Merge NodeSets. Run again, merge RelationshipSets.

        This function is used when memory is limited to avoid collecting too much data in memroy.
        """
        for parser in self.parsers:
            parser.run_with_mounted_arguments()
            for ns in parser.container.nodesets:
                ns.merge(graph)
            parser._reset_parser()

        # run again to create relationships
        for parser in self.parsers:
            parser.run_with_mounted_arguments()
            for rs in parser.container.relationshipsets:
                rs.merge(graph)
            parser._reset_parser()

    def create_index(self, graph:Graph):
        """
        Create all indices.

        :param graph: py2neo.Graph
        """
        for parser in self.parsers:
            for ns in parser.container.nodesets:
                ns.create_index(graph)
            for rs in parser.container.relationshipsets:
                rs.create_index(graph)

    def run_and_merge(self, graph: Graph):
        """
        Run all parser, merge all NodeSets, merge all RelationShip sets.
        """
        self._reset()
        self.run_with_mounted_arguments()
        self.merge(graph)

    def run_and_create(self, graph: Graph):
        """
        Run all parser, merge all NodeSets, merge all RelationShip sets.
        """
        self._reset()
        self.run_with_mounted_arguments()
        self.create(graph)
