from py2neo import Graph
from multiprocessing import Pool
import logging
import os

from typing import List, Union

from graphpipeline.parser import Parser
from graphpipeline.parser.parser import run_parser_merge_nodes, run_and_serialize

log = logging.getLogger(__name__)


class ParserSet:
    """
    A container for a set of Parser objects.

    Used to run batch operations: Merge all, export all to CSV etc.
    """

    def __init__(self):
        self.parsers = []
        self._parser_stash = []

    def add(self, parser: Parser):
        """
        Add a Parser to this ParserSet.

        :param parser: The parser, a subclass of graphpipeline.parser.Parser
        """
        self.parsers.append(parser)

    def select(self, parser: List[Union[str, type]] = None):
        """
        Select a list of parsers by either name or parser instance, stash the others.

        :param parser: List of parsers.
        """
        # select the parser that are passed
        active_parsers = []
        for selection in parser:
            if isinstance(selection, str):
                for p in self.parsers:
                    if p.__class__.__name__ == selection:
                        active_parsers.append(p)
            elif isinstance(selection, type):
                for p in self.parsers:
                    if isinstance(p, selection):
                        active_parsers.append(p)
        # stash the others
        for p in self.parsers:
            if p not in active_parsers:
                self._parser_stash.append(p)
        # set list of active parsers
        self.parsers = active_parsers

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
        log.debug("Merge relationships")
        for p in self.parsers:
            log.debug(f"Merge relationships for {p.__class__.__name__}")
            for relset in p.container.relationshipsets:
                log.debug(f"Merge {str(relset)}")
                relset.create_index(graph)
                relset.merge(graph)

    def merge_nodes(self, graph):
        log.debug("Merge nodes")
        for p in self.parsers:
            log.debug(f"Merge nodes for {p.__class__.__name__}")
            for nodeset in p.container.nodesets:
                log.debug(f"Merge {str(nodeset)}")
                log.debug(f"Number of nodes: {len(nodeset.nodes)}")
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

    def run_and_merge_nodes_parallel(self, graph: Graph, import_path: str, root_dir: str, pool_size=4):
        log.debug(f"Run parallel, pool size {pool_size}")
        graph_config = (graph.service.profile, graph.name)
        pool = Pool(pool_size)
        results = []
        for parser in self.parsers:
            log.debug(f"Append {parser.__class__.__name__} to pool")
            log.debug((graph_config, parser.__class__.__name__, import_path, parser.get_arguments(), [dsi.to_dict() for dsi in parser.datasource_instances], root_dir))
            results.append(
                pool.apply_async(
                    run_parser_merge_nodes, (graph_config, parser.__class__.__name__, import_path, parser.get_arguments(), [dsi.to_dict() for dsi in parser.datasource_instances], root_dir)
                )
            )
        log.debug("Wait for pool to close and join.")
        [r.wait() for r in results]

        pool.close()
        pool.join()

    def run_and_serialize_parallel(self, target_dir: str, import_path: str, root_dir: str, pool_size=4):
        log.debug(f"Run parallel, pool size {pool_size}")

        pool = Pool(pool_size)
        results = []
        for parser in self.parsers:
            log.debug(f"Append {parser.__class__.__name__} to pool")
            log.debug((target_dir, parser.__class__.__name__, import_path, parser.get_arguments(), [dsi.to_dict() for dsi in parser.datasource_instances], root_dir))
            results.append(
                pool.apply_async(
                    run_and_serialize, (target_dir, parser.__class__.__name__, import_path, parser.get_arguments(), [dsi.to_dict() for dsi in parser.datasource_instances], root_dir)
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

    def run_and_serialize(self, target_dir):
        for p in self.parsers:
            p.run_with_mounted_arguments()
            p.serialize(target_dir)

    def serialize(self, target_dir: str) -> None:
        """
        Serialize the entire ParserSet to a directory.

        :param target_dir: The target directory (must exist on disk)
        """
        for p in self.parsers:
            p.serialize(target_dir)

    @classmethod
    def deserialize(self, source_dir: str, whitelist: List[Union[str, type]] = None) -> 'ParserSet':
        log.debug(f"Read ParserSet from {source_dir}")
        ps = ParserSet()
        parser_name_list = [x for x in os.listdir(source_dir) if not x.startswith('.')]

        selected_parser_name_list = set()

        # select the parser for whitelist
        for parser_name in parser_name_list:
            p_with_metadata = Parser.deserialize(os.path.join(source_dir, parser_name), metadata_only=True)
            if whitelist:
                for whitelisted_parser in whitelist:

                    if isinstance(whitelisted_parser, str):
                        if p_with_metadata.name == whitelisted_parser:
                            selected_parser_name_list.add(parser_name)
                    elif isinstance(whitelisted_parser, type):

                        if whitelisted_parser.__name__ == p_with_metadata.name:
                            selected_parser_name_list.add(parser_name)
            else:
                selected_parser_name_list.add(parser_name)

        for selected_parser in selected_parser_name_list:
            p = Parser.deserialize(os.path.join(source_dir, selected_parser))
            ps.add(p)

        return ps
