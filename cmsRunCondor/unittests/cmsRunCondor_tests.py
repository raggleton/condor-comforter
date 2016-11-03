#!/usr/bin/env python

"""
Unittests for cmsrunCondor.py

"""


import unittest
import sys
import os
sys.path.append(os.path.join(os.getcwd(), '..'))
import cmsRunCondor as crc


class cmsRunCondorTests(unittest.TestCase):

    def test_run_range_parser(self):
        self.assertEqual(crc.parse_run_range('1234,5678'), [1234, 5678])
        self.assertEqual(crc.parse_run_range('1234-1236'), [1234, 1235, 1236])
        self.assertEqual(crc.parse_run_range('1234,1236-1238'), [1234, 1236, 1237, 1238])
        self.assertEqual(crc.parse_run_range(''), [])
        self.assertEqual(crc.parse_run_range(None), None)


if __name__ == "__main__":
    unittest.main()