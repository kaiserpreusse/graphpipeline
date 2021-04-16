from py2neo import Graph
from multiprocessing import Pool
import logging

from graphpipeline.parser import Parser
from graphpipeline.parser.parser import run_parser_merge_nodes

log = logging.getLogger(__name__)


class ParserSet:
    """
    A container for a set of Parser objects.

    Used to run batch operations: Merge all, export all to CSV etc.
    """

    def __init__(self):
        self.parsers = []

    def add(self, parser: Parser):
        """
        Add a Parser to this ParserSet.

        :param parser: The parser, a subclass of graphpipeline.parser.Parser
        """
        self.parsers.append(parser)

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

    def run_and_merge_nodes_parallel(self, graph: dict, import_path: str, pool_size=4):
        log.debug("Run parallel")
        graph_config = (graph.service.profile, graph.name)
        pool = Pool(pool_size)
        results = []
        for parser in self.parsers:
            log.debug(f"Append {parser.__class__.__name__} to pool")
            results.append(
                pool.apply_async(
                    run_parser_merge_nodes, (graph_config, parser.__class__.__name__, import_path, parser.get_arguments(), [dsi.to_dict() for dsi in parser.datasource_instances])
                )
            )
        log.debug("Wait for pool to close and join.")
        [r.wait() for r in results]

        pool.close()
        pool.join()

    def run_and_merge_relationships_sequential(self, graph: Graph):
        """
        Merge NodeSets. Run again, merge RelationshipSets.

        This function is used when memory is limited to avoid collecting too much data in memroy.
        """
        # run again to create relationships
        for parser in self.parsers:
            parser.run_with_mounted_arguments()
            for rs in parser.container.relationshipsets:
                rs.merge(graph)
            parser._reset_parser()

    def run_and_merge_sequential(self, graph: Graph):
        """
        Merge NodeSets. Run again, merge RelationshipSets.

        This function is used when memory is limited to avoid collecting too much data in memroy.
        """
        log.debug("Run and merge sequential.")
        log.debug("Merge NodeSets")
        for parser in self.parsers:
            log.debug(f"Run {parser.__class__.__name__}")
            parser.run_with_mounted_arguments()
            for ns in parser.container.nodesets:
                log.debug(f"Merge NodeSet with {ns.labels}, {ns.merge_keys}")
                ns.merge(graph)
            parser._reset_parser()

        # run again to create relationships
        log.debug("Merge RelationshipSets")
        for parser in self.parsers:
            log.debug(f"Run {parser.__class__.__name__}")
            parser.run_with_mounted_arguments()
            for rs in parser.container.relationshipsets:
                log.debug(f"Merge RelationshipSet {rs}")
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
