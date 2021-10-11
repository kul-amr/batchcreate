import unittest
import logging
import sys
from batching import BatchCreator

logger = logging.getLogger(__name__)


class BatchCreatorTestCase(unittest.TestCase):
    """
    Tests BatchCreator class methods.
    This class defines a common `setUp` method that defines attributes which are used
    in the various tests.
    """

    def setUp(self):
        logging.basicConfig(level=logging.DEBUG)


    def test_invalid_record_size(self):
        records = ['aaa', 'bbb', 'largerecordinput', 'ddd', 'eee', 'fff', 'ggg']
        # logger.debug(sys.getsizeof(records[0]))
        self.batches = BatchCreator(records,
                                    max_record_size=60,
                                    max_batch_size=200,
                                    max_batch_num_records=4).batches()

        self.assertEqual(len(self.batches), 2)
        self.assertEqual(self.batches, [['aaa', 'bbb', 'ddd', 'eee'], ['fff', 'ggg']])


    def test_invalid_batch_size(self):
        records = ['aaa', 'bb', 'cc', 'ddd']
        self.batches = BatchCreator(records,
                                    max_record_size=60,
                                    max_batch_size=130,
                                    max_batch_num_records=4).batches()

        self.assertEqual(len(self.batches), 3)
        self.assertEqual(self.batches, [['aaa'], ['bb', 'cc'], ['ddd']])

    def test_batch_num_records(self):
        records = ['aaa', 'bbb', 'ccc', 'ddd', 'eee', 'fff', 'ggg']
        self.batches = BatchCreator(records,
                                    max_record_size=100,
                                    max_batch_size=500,
                                    max_batch_num_records=4).batches()

        self.assertEqual(len(self.batches), 2)

        batch_0 = self.batches[0]
        batch_1 = self.batches[1]
        self.assertEqual(len(batch_0), 4)
        self.assertEqual(len(batch_1), 3)

        self.assertEqual(self.batches, [['aaa', 'bbb', 'ccc', 'ddd'], ['eee', 'fff', 'ggg']])

    def test_default_limits(self):
        records = ['aaa', 'bbb', 'ccc', 'ddd', 'eee', 'fff', 'ggg']
        self.batches = BatchCreator(records).batches()

        self.assertEqual(len(self.batches), 1)
        self.assertEqual(self.batches, [['aaa', 'bbb', 'ccc', 'ddd', 'eee', 'fff', 'ggg']])

    def test_empty_input_list(self):
        records = []
        self.batches = BatchCreator(records).batches()

        self.assertEqual(len(self.batches), 1)
        self.assertEqual(self.batches, [[]])



if __name__ == '__main__':
    unittest.main()
