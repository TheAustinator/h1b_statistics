from collections import Counter, namedtuple
import csv
from datetime import datetime
from dateutil import parser
import operator as op


class CleanReader:
    def __init__(self, file_path, cols, alias_dict=None, preprocessing='strict'):
        """
        Iterable csv reader wrapper that cleans entries in columns of interest
        the cleaned entries in a namedtuple. The namedtuple is named either by
        the column names, or by the aliases specified in the alias dict. The
        aliases are intended to provide a common and convenient syntax when
        rows are coming from data sources with variable column names.

        Args:
            file_path: path to csv file to read
            cols: columns names of interest, as they appear in the csv file
            alias_dict(dict, None): column names (cols) as keys and aliases as
                values. The aliases will be used as the keys for the namedtuple
                that is yielded. If None, column names will be used as keys in
                the namedtuple.

        Returns[__iter__]:
            row_tup(namedtuple): aliases or column names as keys and
                corresponding entries from the current row of the csv as values

        Example Usage:
            >>>alias_dict = {
            >>>    'party': ['DEM_REP', 'POL_PARTY', 'CANDIDATE_PARTY'],
            >>>    'age': ['AGE', 'VOTER_AGE'],
            >>>    'education': ['HIGHEST_EDUCATION', 'EDU', 'EDUCATION'],
            >>>}
            >>>cols = ['DEM_REP', 'VOTER_AGE', 'EDUCATION']
            >>>reader = CleanReader('./election_data.csv', alias_dict):
            >>>dem_ages = Counter()
            >>>rep_ages = Counter()
            >>>for row in reader:
            >>>    if row.party == 'democrat':
            >>>        dem_ages[row.age] += 1
            >>>    if row.party == 'republican':
            >>>        rep_ages[row.age] += 1
            >>>dem_ages
            {'18-24': 5573, '25-34': 7242, '35-44': 5223, '45-54': 3531,
             '55-64': 3146, '65+': 7341,}
        """
        self.file_path = file_path
        self.cols = list(cols)
        if alias_dict:
            self.aliases = [alias_dict[col] for col in self.cols]
        else:
            self.aliases = cols
        self.preprocessing = preprocessing

    def __iter__(self):

        self._len = 0
        try:
            RowTup = namedtuple('RowTup', self.aliases)
        except:
            import ipdb;ipdb.set_trace()
        with open(self.file_path, 'rU') as f:
            self.reader = csv.DictReader(f, delimiter=';')
            for row in self.reader:
                self._len += 1
                # select desired cols, preprocess text, and yield named_tuple
                row_data = (self.preprocess_text(row[col]) for col in self.cols)
                row_tup = RowTup(*row_data)
                yield row_tup

    def preprocess_text(self, entry):
        """
        Precprocess the text of a single entry in a row of a csv.
        It is encoded and decoded to ensure integrity, and by default, an
        exception is raised if there is an error in this step (errors
        parameter). It is then stripped of leading and trailing spaces and
        converted to lowercase.

        Args:
            entry: entry from a row of a csv file
            errors: corresponds to the errors parameter of str.encode

        Returns:
            entry: preprocessed entry
        """
        entry = entry.encode('ascii', errors=self.preprocessing).decode()
        entry = entry.strip()
        entry = entry.lower()
        return entry


class ExtendedCounter(Counter):
    """
    A Counter object that has been extended to calculate the fraction of total
    of a specific entry and the percent of total as a string.
    """
    def fraction(self, item):
        item = item.lower()
        return self[item] / sum(self.values())

    def percent_str(self, item):
        item = item.lower()
        return f'{(100 * self.fraction(item)):.1f}%'


class MultiFileCounter:
    Constraint = namedtuple('Constraint', ['alias', 'cmp', 'val'])
    cmp_dict = {
        '==': op.eq,
        '<=': op.le,
        '>=': op.ge,
        '<': op.lt,
        '>': op.gt,
    }

    def __init__(self, files, colname_list_dict=None):
        """
        A counter for entries in specified columns of csv files that can keep
        continuous count over multiple files, count only when a comparator is
        satisfied, and use aliases for column names for cases in which column
        names vary between files or are named inconveniently. Counters are
        added via the add_counter method, and comparator constraints are added
        via the add_constraint method. Both constraints and counters can be
        added via the

        Args:
            files (iterable[str]): paths to csv files to be read

            colname_list_dict (dict[str, list]): universal column aliases as
                keys and lists of possible column names as values. The aliases
                provide a common interface for columns that may have different
                names in different files. The column names should be included
                in the key.
                ex:
                    {
                        'population': ['POP_METRO', 'POPULATION', 'POP_TOT'],
                        'income avg': ['HOUSEHOLD_INC', 'AVG_INCOME', 'SALARY_AVG'],
                    }

        Example Usage:
            >>>colname_list_dict = {
            >>>    'date': ['DATE', ]
            >>>}
            >>>mfc = MultiFileCounter(my_files, my_colname_list_dict)
            >>>mfc.add_constraint('date', op.gt, 20180601)
            >>>mfc.add_constraint('network', op.eq, 'comedy central')
            >>>mfc.add_counter('program')
            >>>mfc.add_counter('length')
            >>>mfc.count()
            >>>mfc.constraints
            [Constraint(alias='date', cmp=op.gt, val=20180601),
             Constraint(alias='network', cmp=op.eq, val='comedy central)]
            >>>mfc.counters
            {'program': {'south park': 39, 'colbert report': 21, 'family guy'...}
             'length': {'30': 95, '60': 74, '90': 33, '15': 13 ...}}
             >>>mfc.file_parsed
             16

        """
        self.files = files
        self.colname_list_dict = colname_list_dict

        self._counters = dict()
        self._constraints = list()
        self._files_parsed = 0
        self._alias_dict = dict()

    def __iter__(self):
        for file in self.files:
            alias_dict = self.alias_dict(file)
            colname_dict = self.colname_dict(file)
            colnames = [colname_dict[alias] for alias in self.aliases]
            clean_reader = CleanReader(file, colnames, alias_dict)
            for row_tup in clean_reader:

                # update counters if all constraints are satisfied
                if self.constraints_satisfied(row_tup):
                    for alias in self.counters:
                        self._counters[alias][getattr(row_tup, alias)] += 1

            self._files_parsed += 1
            yield

    def __len__(self):
        return len(self.files)

    @property
    def files_parsed(self):
        """number of files parsed at the given time"""
        return self._files_parsed

    @property
    def aliases(self) -> set:
        """
        aliases for column names used to establish a commmon and convenient
        name for columns that may have different names in different files or
        inconvenient naming conventions.

        These are the columns that will be parsed from the csv, and are the
        combined set of all counter and constraint columns. The object must be
        initialized with a colname_list_dict if the strings passed to the
        add_constraint and add_counter do not correspond to a single column
        which is present in all of the csvs specified by the 'files' parameter
        """
        return self.colname_list_dict.keys()

    def add_alias(self, alias, colname):
        """
        Add a new alias for a column name. Modifies self.colname_list_dict.

        Example Usage:
            >>>mfc.add_alias('population', 'TOT_POP')
        """
        if not self.colname_list_dict:
            self.colname_list_dict = dict()

        if alias in self.colname_list_dict:
            self.colname_list_dict[alias].append(colname)
        else:
            self.colname_list_dict.update({alias: [colname]})

    @property
    def constraints(self):
        """
        List of comparator constraints of values of specified aliases or
        columns. For rows which all constraints are satisfied are counted by
        the counters, and are skipped otherwise.

        Constraints are namedtuple objects, with attributes alias, cmp, and
        val, which correspond to the class attribute namedtuple 'Constraint'.
        """
        return self._constraints

    def add_constraint(self, alias, cmp, val):
        """
        Add a new constraint which controls whether or not a row is counted.
        The constraints compare the value of a row at a specified alias or
        column to a specified value using a specified comparator.
        Args:
            alias (str): alias for column names to be constrained. Must
                correspond to an alias or a column name that is present in all
                files
            cmp (str): string representation of comparator operator.
                Options: '==', '<=', '>=', '<', '>'
            val: value against which to compare row at column

        Example Usage:
            >>>mfc.add_constraint('population', '>', 10,000),
            >>>mfc.add_constraint('latitude', '<', 40),
            >>>mfc.add_constraint('region', '==', 'europe'),
        """
        cmp_op = self.cmp_dict[cmp]
        constraint = self.Constraint(alias, cmp_op, val)
        self._constraints.append(constraint)

    def constraints_satisfied(self, row):
        """
        Checks whether all constraints are satisfied
        Args:
            row (namedtuple): row containing only columns of interest

        Returns:
            True or False
        """
        if not self.constraints:
            return True
        else:
            bools = []
            for con in self.constraints:
                entry = getattr(row, con.alias)
                # convert row entry to numeric if val is numeric
                if isinstance(con.val, float) or isinstance(entry, int):
                    entry = float(entry)
                elif isinstance(con.val, datetime):
                    entry = parser.parse(entry)
                bool_ = con.cmp(entry, con.val)
                bools.append(bool_)
            return all(bools)

    @property
    def counters(self) -> dict:
        """
        Counter dictionary with aliases as keys and ExtendedCounters as values
        """
        return self._counters

    def add_counter(self, counter_alias):
        """
        Add a new counter which corresponds to an alias
        Args:
            counter_alias (str): must match an alias or column name

        Example Usage:
            >>>mfc.add_counter('region')
            >>>mfc.count()
            >>>mfc.counters()
            {'region':{'asia': 67, 'europe': 43, 'north america': 21...}}
        """
        self._counters[counter_alias] = ExtendedCounter()

    def count(self):
        """
        Runs counter on all files by iterating through self
        Returns:
            self.counters
        """
        for file in self:
            continue

    def alias_dict(self, file):
        """
        Dictionary used to lookup aliases from column names for a specific
        file. Column names from file as keys, aliases as values.
        Args:
            file (str): path to file
        """
        with open(file) as f:
            reader = csv.reader(f, delimiter=';')
            header = next(reader)

        if self.colname_list_dict:
            alias_dict = dict()
            for alias in self.aliases:
                colname_list = self.colname_list_dict[alias]
                colname = next(col for col in header if col in colname_list)
                alias_dict.update({colname: alias})
        else:
            alias_dict = {alias: alias for alias in self.aliases}

        return alias_dict

    def colname_dict(self, file):
        """
        The opposite of alias dict. Dictionary used to lookup column names
        from aliases for a specific file. Aliases as keys and column names
        from file as values.
        Args:
            file (str): path to file
        """
        alias_dict = self.alias_dict(file)
        colname_dict = {alias: colname for colname, alias in alias_dict.items()}
        return colname_dict
