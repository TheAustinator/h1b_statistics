import csv
import os
import pytest
import sys
from unittest import TestCase

sys.path.insert(0, '../../../src')
from h1b_counting import main
from iterators import CleanReader, MultiFileCounter


def run_tests():
    test_output_size()
    reset()
    test_idempotency()
    reset()
    test_mfc = TestMultiFileCounter()
    test_mfc.setUp()
    test_mfc.test_aliases()


def test_output_size():
    main()
    output_1 = 'output/top_10_occupations.txt'
    output_2 = 'output/top_10_states.txt'

    assert(file_len(output_1) == 6)
    assert(file_len(output_2) == 10)


def file_len(filename):
    with open(filename) as f:
        for i, line in enumerate(f):
            pass
    return i + 1


def test_idempotency():
    main()
    main()

    assert(len(os.listdir('output')) == 2)


def reset():
    for file in os.listdir('output'):
        os.remove(os.path.join('output', file))


class TestMultiFileCounter(TestCase):
    files = [os.path.join('./input', file) for file in os.listdir('./input')]

    def setUp(self):
        reset()
        self.mfc = MultiFileCounter(self.files)

    def tearDown(self):
        del self.mfc
        reset()

    def add_aliases(self):
        self.mfc.add_alias('occupation', 'SOC_NAME')
        self.mfc.add_alias('job_title', 'JOB_TITLE')

    def test_aliases(self):
        # try adding aliases without initial colname_dict
        self.add_aliases()

        # try starting with colname_dict and adding alias
        colname_dict = {'occupation': ['SOC_NAME']}
        mfc = MultiFileCounter(self.files, colname_dict)
        mfc.add_alias('job_title', 'JOB_TITLE')

        # check aliases and that they can be used to access counters
        self.assertEqual(mfc.aliases, self.mfc.aliases)
        import ipdb; ipdb.set_trace()
        self.assertEqual(mfc.aliases, {'occupation', 'job_title'})
        self.mfc.add_counter('occupation')
        self.mfc.add_counter('job_title')
        self.mfc.count()
        try:
            counter_1 = self.mfc.counters['occupation']
            counter_2 = self.mfc.counters['job_title']
        except Exception as e:
            raise AssertionError(
                f'Cannot extract counters from mfc by alias {e.args}'
            )


if __name__ == '__main__':
    run_tests()
