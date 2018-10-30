from argparse import ArgumentParser
import os
import sys

from iterators import MultiFileCounter


def main():
    """
    Reads files(s) specified by first argument. If first argument is a
    directory, all files in the directory will be read and iterated over by
    a MultiFileCounter, and if it is a single csv file, just that file will be
    parsed.
    The second two command line arguments are the names of the output files:
    top_10_occupations.txt and top_10_states.txt. It uses the MultiFileCounter
    to iterate through all of the files sequentially, reading and cleaning
    relevant entries with the CleanReader, and counting the entries for
    each occupation SOC code and the entries for the state where the work took
    place. Only rows that have certified status are counted. The top 10
    occupations and states with the highest counts are output into their
    respective text files with the item, count, and percentage of total.
    """

    input_ = str(sys.argv[1])
    if input_.endswith('.csv'):
        input_files = [input_]
    else:
        input_files = [os.path.join(input_, file) for file in os.listdir(input_)]

    output_filepath_1 = str(sys.argv[2])
    output_filepath_2 = str(sys.argv[3])
    header_1 = ('TOP_OCCUPATIONS', 'NUMBER_CERTIFIED_APPLICATIONS', 'PERCENTAGE')
    header_2 = ('TOP_STATES', 'NUMBER_CERTIFIED_APPLICATIONS', 'PERCENTAGE')

    # possible column name aliases
    # note: this step could also be done dynamically after MultiFileCounter
    #       initialization with:
    #   mfc.add_alias('status', 'STATUS')
    #   mfc.add_alias('status', 'CASE_STATUS')
    #   ...
    colname_dict = {
        'status': ['STATUS', 'CASE_STATUS'],
        'occupation': ['SOC_NAME', 'LCA_CASE_SOC_NAME'],
        'state': ['LCA_CASE_WORKLOC1_STATE', 'WORKSITE_STATE'],
    }

    mfc = MultiFileCounter(input_files, colname_dict)
    mfc.add_constraint('status', '==', 'certified')
    mfc.add_counter('occupation')
    mfc.add_counter('state')
    mfc.count()
    counter_1 = mfc.counters['occupation']
    counter_2 = mfc.counters['state']
    write_output_file(output_filepath_1, header_1, counter_1, 10, ';')
    write_output_file(output_filepath_2, header_2, counter_2, 10, ';')


def write_output_file(filepath, header, counter, n, delimiter):
    """
    Write text files with specified header and body that is written from top
    n entries in a counter object.
    Args:
        filepath (str): path to output file
        header (iterable): column names for header
        counter (iterators.ExtendedCounter): used for text body
        n (int): top n entries of counter to use in text
        delimiter (str): delimiter for text file
    """
    header_text = ';'.join(header) + '\n'
    body_text = gen_counter_text(counter, n, delimiter)

    with open(filepath, 'w') as f:
        f.write(header_text)
        for row in body_text:
            f.write(row)


def gen_counter_text(counter, n, delimiter):
    """
    Generates body text for top n entries in counter of the format:
        name; count; percentage
    Args:
        counter (iterators.ExtendedCounter): used for text body
        n (int): top n entries of counter to use in text
        delimiter (str): delimiter for text file

    Returns:
        text_gen (generator): yields lines of text

    Example Usage:
        >>>text_gen = gen_counter_text(my_counter, 10, ',')
        >>>next(text_gen)
        'italy,652,32.0%\n'
    """
    unsorted_top_n = counter.most_common(n)
    top_n = sorted(unsorted_top_n, key=lambda x: (-x[1], x[0]))
    names = [tup[0].upper() for tup in top_n]
    counts = [str(tup[1]) for tup in top_n]
    percentage = [counter.percent_str(name) for name in names]
    data = zip(names, counts, percentage)
    text_gen = (delimiter.join(entry for entry in row) + '\n' for row in data)
    return text_gen


if __name__ == '__main__':
    main()
