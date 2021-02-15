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
        for p in self.parsers:
            for nodeset in p.container.nodesets:
                nodeset.create_index(graph)
                nodeset.merge(graph)
        for p in self.parsers:
            for relset in p.container.relationshipsets:
                relset.create_index(graph)
                relset.merge(graph)

    def create(self, graph):
        """
        Fist merge all NodeSets, then merge all RelationshipSets in the ParserSet.
        """
        for p in self.parsers:
            for nodeset in p.container.nodesets:
                nodeset.create_index(graph)
                nodeset.create(graph)
        for p in self.parsers:
            for relset in p.container.relationshipsets:
                relset.create_index(graph)
                relset.create(graph)

    def _reset(self):
        for p in self.parsers:
            p._reset_parser()

    def merge_sequential(self, graph: Graph):
        """
        Run parsers one by one, run_and_merge NodeSets. Run again, run_and_merge RelationshipSets.

        This function is used when memory is limited to avoid collecting too much data in memroy.
        """
        pass

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
