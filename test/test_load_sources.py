from __future__ import print_function
import unittest
from rowpack import RowpackReader, RowpackWriter, Schema, ingest
from os.path import dirname, join
import json

class TestBasic(unittest.TestCase):

    def test_basic(self):
        sources_path = join(dirname(__file__),'test_data','all_sources.json')

        with open(sources_path) as f:
            sources = json.load(f)

        for s in sources:
            ref = s.get('ref')
            if ref and ref.startswith('http'):
                print(ref, s.keys())

if __name__ == '__main__':
    unittest.main()
