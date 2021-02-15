"""
- Different datasources use slightly different versions of the EMBL file format.
- An EMBL file container (_EmblLikeFileContainer, sub classes) is called with the
  file and a sequence record parser (_BaseSequenceRecordParser, sub classes) with dependency injection
- The file container class uses the parser to parse the file, the parser returns a SequenceRecord object.

"""
import logging

log = logging.getLogger(__name__)


##############################################################
# Container classes for specific EMBL like files
##############################################################

class _EmblLikeFileContainer(object):
    """
    Different datasources use slightly different versions of the EMBL file format.

    This is the parent class for specific EMBL file containers.

    A container is called with the file and the corresponding _RealEMBLSequenceRecordParser.
    """

    def __init__(self, f, SeqRecordParser):
        self.f = f
        self.SeqRecordParser = SeqRecordParser

    @property
    def records(self):
        """
        Yield records of EMBL file.
        """
        this_record = []

        for l in self.f:

            if l.startswith('//'):
                yield self.SeqRecordParser(this_record).parse()
                this_record = []

            else:
                l = l.rstrip()
                this_record.append(l)

        yield self.SeqRecordParser(this_record).parse()

    @property
    def record_count(self):
        """
        Count number of records.

        :return: Number of records in file.
        """
        counter = 0
        for l in self.f:
            if l.startswith('//'):
                counter += 1
        # reset file buffer position to 0
        self.f.seek(0)

        return counter


class EMBL(_EmblLikeFileContainer):
    """
    Container for properly formatted EMBL files.
    """

    def __init__(self, f):
        super(EMBL, self).__init__(f, _RealEMBLSequenceRecordParser)


class EMBLReaderUniProt(_EmblLikeFileContainer):
    """
    Container for UniProt data files. Some differences to nucleotide EMBL format (e.g. feature table).
    """

    def __init__(self, f):
        super(EMBLReaderUniProt, self).__init__(f, _UniprotSequenceRecordParser)


##############################################################
# Sequence record parser classes
##############################################################

def base_ID(lines):
    """
    Extract the ID and additional information from ID line. Always one line per record.

    ID   hsa-let-7a-1      standard; RNA; HSA; 80 BP.

    ID   BN000065; SV 1; linear; genomic DNA; STD; HUM; 315242 BP.

    :param lines: Lines to parse.
    :return: ID line
    :rtype: str
    """
    id_lines = _get_lines_prefix(lines, 'ID')
    if len(id_lines) == 1:
        # get first element of ID field, first split by ';'
        # then split by whitespace in case additional columns are delimited by multiple whitespaces
        first_part = id_lines[0].split(';')[0].split()[0].strip()
        return 'ID', first_part


def base_AC(lines):
    """
    Example:
    AC   P31946; A8K9K2; E1P616;

    :param lines:
    :return: List of accessions.
    :rtype: list[str]
    """
    accessions = [val.strip() for val in _merge_lines_prefix(lines, 'AC').split(';') if val]
    return 'AC', accessions


def base_OS(lines):
    """
    Example:
    OS   Homo sapiens (Human).

    Can be multiple lines.

    :param lines: Lines to parse.
    :return:
    """
    return 'OS', _merge_lines_prefix(lines, 'OS')


def base_OC(lines):
    """
    Example:
    OC   Eukaryota; Metazoa; Chordata; Craniata; Vertebrata; Euteleostomi;
    OC   Mammalia; Eutheria; Euarchontoglires; Primates; Haplorrhini;
    OC   Catarrhini; Hominidae; Homo.

    :param lines: Lines to parse.
    :return: Organims classification.
    :rtype: str
    """
    return 'OC', _merge_lines_prefix(lines, 'OC')


def base_DE(lines):
    return 'DE', _merge_lines_prefix(lines, 'DE')


def base_DR(lines):
    """
    Example:
    DR   DNASU; 7529; -.
    DR   Ensembl; ENST00000353703; ENSP00000300161; ENSG00000166913. [P31946-1]
    DR   Ensembl; ENST00000372839; ENSP00000361930; ENSG00000166913. [P31946-1]

    :param lines: Lines to parse.
    :return: Database Xrefs.
    :rtype: list[tuple]
    """
    xrefs = []
    for l in _get_lines_prefix(lines, 'DR'):
        db_name, main_ref_id = _split_xref_line(l)
        xrefs.append((db_name, main_ref_id))

    return 'DR', xrefs


def base_references(lines):
    reference_blocks = []

    # collect references
    cur_ref = []
    for l in [l for l in lines if l.startswith('R')]:
        # RN marks beginning of new reference
        if l.startswith('RN'):
            # add last ref to list of refs if not the first one
            if cur_ref:
                reference_blocks.append(cur_ref)
            # start new ref
            cur_ref = []
        # append line to collection list
        cur_ref.append(l)

    # add last element to list of refs
    reference_blocks.append(cur_ref)

    result_references = []
    # process references
    for ref_lines in reference_blocks:
        ref_data = {}

        # only process external references
        if any([x.startswith('RX') for x in ref_lines]):
            # get xrefs
            ref_data['xref'] = []
            for x in _get_lines_prefix(ref_lines, 'RX'):
                ref_data['xref'].extend(_split_equal_sign_xref(x))

            ref_data['title'] = _merge_lines_prefix(ref_lines, 'RT')
            ref_data['author'] = _merge_lines_prefix(ref_lines, 'RA')
            ref_data['comment'] = _merge_lines_prefix(ref_lines, 'RC')
            ref_data['reference'] = _merge_lines_prefix(ref_lines, 'RL')
            ref_data['number'] = _get_lines_prefix(ref_lines, 'RN')[0].replace('[', '').replace(']', '')

            result_references.append(ref_data)

    return 'references', result_references


def base_features(lines):
    """
    Feature tables are different in other EMBL like file formats.

    This parser works with standard nucleotide EMBL format.
    """

    feature_list = []

    # collect features
    cur_feature = []
    for l in lines:
        if l.startswith('FT'):

            # find lines that start with a key and identify beginning of new feature
            # other lines start with 'FT         ' (mulitple whitespaces)
            if not l.startswith('FT         '):
                if cur_feature:
                    feature_list.append(cur_feature)
                # start new feature
                cur_feature = []
            # append line to collection list
            cur_feature.append(l)

    # add last feature
    feature_list.append(cur_feature)

    # process features
    for f_lines in feature_list:
        feature_data = {'qualifier': {}}
        feature_xrefs = []

        full_feature_segment = _RecordSegment(f_lines)

        # qualifier segment is generated from everything which is not considered a header line
        qualifier_segment = _RecordSegment(f_lines[1:])

        # iterate all lines until first qualifier is found
        for i, l in enumerate(full_feature_segment.line_values):
            if not l.startswith('/'):
                # first line contains key
                if i == 0:
                    key, location = l.split()
                    feature_data['key'] = key
                    feature_data['location'] = location
                # following lines are appended to the location
                else:
                    feature_data['location'] += l
            # put everything else in qualifier segement
            else:
                qualifier_segment = _RecordSegment(f_lines[i:])
                break

        cur_q_key = None

        # process qualifiers
        for q in qualifier_segment.line_values:

            if q.startswith('/'):
                q = q[1:]
                q_fields = q.split('=')
                cur_q_key = q_fields[0]
                cur_q_value = q_fields[1].strip('"')

                # sub structure db xrefs
                if cur_q_key == 'db_xref':
                    q_xref_db, q_xref_id = cur_q_value.split(':', 1)
                    feature_xrefs.append((q_xref_db, q_xref_id))
                else:
                    feature_data['qualifier'][cur_q_key] = cur_q_value

            else:
                if cur_q_key:
                    feature_data['qualifier'][cur_q_key] += q

        # add feature xrefs if existing
        if feature_xrefs:
            feature_data['xref'] = feature_xrefs

        return 'features', feature_data


class _BaseSeqRecordParser(object):
    def __init__(self, lines):
        self.lines = lines

        self.seqrecord = {}
        self._parse_functions = []

    def parse(self):
        """
        Call all parser functions associated with this class and collect results.
        :return: The SeqenceRecord with results.
        """
        for function in self._parse_functions:
            try:
                key, parse_result = function(self.lines)
                self.seqrecord[key] = parse_result
            except ValueError:
                log.warning('Parsing function does not return key/result. Returns: {0}'.format(
                    function(self.lines)
                ))
            except TypeError:
                log.debug('Parsing function on empty list of lines?')
                log.debug(self.lines)
        return self.seqrecord


class _UniprotSequenceRecordParser(_BaseSeqRecordParser):
    """
    Sequence record parser for UniProt files. The feature tables (FT) look different from DNA seq EMBL files.

    Some fields have specific values.
    """

    def __init__(self, lines):
        super(_UniprotSequenceRecordParser, self).__init__(lines)

        # collect functions for parsing
        self._parse_functions = [
            base_ID, base_AC, base_OS, base_OC, base_DE, base_DR, base_references
        ]


class _RealEMBLSequenceRecordParser(_BaseSeqRecordParser):
    def __init__(self, lines):
        super(_RealEMBLSequenceRecordParser, self).__init__(lines)

        # collect functions for parsing
        self._parse_functions = [
            base_ID, base_AC, base_OS, base_OC, base_DE, base_DR, base_references, base_features
        ]


##############################################################
# Helper functions for parsing
##############################################################

class _RecordSegment(object):
    """
    Helper class to collect some lines of a sequence record.

    Splits lines in ID and Value.

    Used for operations like: Get all lines starting with "FT", run_and_merge all lines starting with "RA".
    """

    def __init__(self, lines):
        self.lines = lines

        self.line_tuples = _get_line_tuples(self.lines)

        self.line_ids = [x[0] for x in self.line_tuples]
        self.line_values = [x[1].strip() for x in self.line_tuples]

    def get(self, ref_line_id):
        """
        Get data for an ID. Multiple lines are returned as list.

        :param ref_line_id: Line ID.
        :return: List of lines with ID.
        :rtype: list[str]
        """
        elmnts = []
        for line_id, line_content in self.line_tuples:
            if line_id == ref_line_id:
                elmnts.append(line_content)
        return elmnts

    def get_one(self, ref_line_id):
        """
        Get content of an identifier that only occurs once in this _RecordSegment.

        Else raise error.

        :param ref_line_id: Line ID.
        :return: Content of the line.
        """
        elmnts = self.get(ref_line_id)

        if len(elmnts) > 1:
            print(elmnts)
            raise AttributeError("More than one line with ID {0}.".format(ref_line_id))
        elif len(elmnts) == 1:
            return elmnts[0]
        else:
            return None

    def merge(self, ref_line_id, join_char=None):
        """
        Merge all lines with a specific line ID and return as string.

        :param ref_line_id: Line ID.
        :param join_char: Character used for joining the lines.
        :return: Merged string of a specific line ID.
        """
        if not join_char:
            join_char = ' '

        return join_char.join(self.get(ref_line_id))


def _get_lines_prefix(lines, line_id):
    """
    Get all lines with line ID.

    :param lines: List of lines to get data.
    :param line_id: Line ID.
    :return: List of lines.
    :rtype: list[str]
    """
    result = []
    for l in lines:
        if l.startswith(line_id):
            _, line_content = _get_line_tuple(l)
            result.append(line_content.strip())

    return result


def _merge_lines_prefix(lines, line_id, join_char=None):
    """
    Merge all lines beginning with a specific ID.

    :param lines: Lines to parse and run_and_merge.
    :param line_id: Line ID to run_and_merge.
    :param join_char: Character for string join.
    :return: Joined lines.
    """
    if not join_char:
        join_char = ' '

    return join_char.join(
        _get_lines_prefix(lines, line_id)
    )


def _get_line_tuples(list_of_lines):
    """
    Get line tuples for a list of lines. Remove 'XX' lines.

    :param list_of_lines: List of unprocessed lines.
    :return: List of tuples with ID and line content.
    """
    cleaned_lines = []
    for l in list_of_lines:
        if not l.startswith('XX'):
            cleaned_lines.append(_get_line_tuple(l))
    return cleaned_lines


def _get_line_tuple(l):
    """
    Get line tuple with ID and data. Remove trailing spaces.

    :param l: A line of the EMBL file.
    :return: Line tuple.
    """
    l = l.rstrip()
    line_id = l[0:2]
    line_content = l[5:]
    return line_id, line_content


def _split_xref_line(l):
    """
    Split a xref line.

        ENTREZGENE; 100315420; MIR576.
        PUBMED; 18186931.

    :param l: A db xref line.
    :return: DB identifier and first ID.
    """
    l = l.strip()

    # there can be data after trailing '.' (comment)
    # only use part before trailing dot
    # note that some IDs contain dots, thus we only split by the LAST dot
    l = l.rsplit('.', 1)[0]

    flds = l.split(';')

    db_name = flds[0].strip()
    ids = [x.strip() for x in flds[1:]]

    return db_name, ids


def _split_equal_sign_xref(l):
    """
    Split xref line used by some files in reference xref:

    PubMed=25944712; DOI=10.1002/pmic.201400617;

    :param l: Reference line.
    :return:
    """

    mapping_tuples = []

    for entry in l.strip().split(';'):
        entry = entry.strip()
        flds = entry.split('=')
        if len(flds) == 2:
            mapping_tuples.append((flds[0], flds[1]))
    return mapping_tuples
