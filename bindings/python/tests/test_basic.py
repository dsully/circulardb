#!/usr/bin/python

import sys
import os
import time
import shutil
import math
import tempfile

sys.path.append('.')

import circulardb
import unittest

class TestCircularDB(unittest.TestCase):

  def setUp(self):
    self.tempdir = tempfile.mkdtemp()
    self.file    = os.path.join(self.tempdir, "basic.cdb")
    self.name    = "Testing Python CDB"

    if os.path.exists(self.tempdir):
      shutil.rmtree(self.tempdir)

    os.makedirs(self.tempdir)

  def tearDown(self):
    if os.path.exists(self.tempdir):
      shutil.rmtree(self.tempdir)
      pass

  def test_create(self):
    cdb = circulardb.Storage(self.file, os.O_CREAT|os.O_RDWR|os.O_EXCL, -1, self.name)

    self.assert_(cdb)
    cdb.max_value = 100
    self.assertEqual(cdb.filename, self.file)
    self.assertEqual(cdb.name, self.name)
    self.assertEqual(cdb.type, "gauge")
    self.assertEqual(cdb.units, "absolute")
    self.assertEqual(cdb.min_value, 0)
    self.assertEqual(cdb.max_value, 100)
    cdb.close

  def test_rw(self):
    records = []
    now     = int(time.time())

    flags       = os.O_CREAT|os.O_RDWR|os.O_EXCL
    mode        = -1
    max_records = 0
    desc        = None
    type        = "gauge"
    units       = "absolute"
    min_value   = 0
    max_value   = 0
    interval    = 300

    for i in range(1, 11):
      records.append([ now, float(i) ])
      now += 1

    cdb = circulardb.Storage(self.file, flags, mode, self.name, desc, max_records, type, units, min_value, max_value, interval)
    self.assert_(cdb)
    self.assertEqual(cdb.max_value, max_value)

    self.assertEqual(10, cdb.write_records(records))

    read = cdb.read_records()

    self.assertEqual(10, len(read))
    self.assertEqual(10, cdb.num_records)

    # Can't use assert_same(records, read) here, because the object_id's will be different.
    for i in range(0, len(records)):
      self.assertEqual(records[i][0], read[i][0])
      self.assertEqual(records[i][1], read[i][1])

    self.assertEqual(5.5, cdb.statistics().mean)
    self.assertEqual(5.5, cdb.statistics().median)
    self.assertEqual(55, cdb.statistics().sum)
    self.assertEqual(10, cdb.statistics().max)
    self.assertEqual(1, cdb.statistics().min)

    # Check setting a float and reading it back.

    self.assertEqual(1, cdb.update_record(records[5][0], 999.0005))

    float_check = cdb.read_records()

    self.assertEqual(float_check[5][1], 999.0005)

    # Check setting value to None
    self.assertEqual(1, cdb.update_record(records[6][0], None))

    none_check = cdb.read_records()

    self.assertEqual(repr(float('nan')), repr(none_check[6][1]))

    cdb.close()

  def test_counter_reset(self):
    cdb = circulardb.Storage(self.file, os.O_CREAT|os.O_RDWR|os.O_EXCL, -1, self.name, None, 0, "counter")
    self.assert_(cdb)

    records = []
    now     = int(time.time())
    i       = 0

    for v in [10, 11, 12, 0, 1]:
      records.append([ now+i, float(v) ])
      i += 1

    self.assertEqual(5, cdb.write_records(records))

    read = cdb.read_records(cooked = 1)

    self.assertEqual(10, read[0][1])
    self.assertEqual(1, read[1][1])
    self.assertEqual(1, read[2][1])
    self.assertEqual(repr(float('nan')), repr(read[3][1]))
    self.assertEqual(1, read[4][1])

    cdb.close()

  def test_step(self):
    cdb = circulardb.Storage(self.file, os.O_CREAT|os.O_RDWR|os.O_EXCL, -1, self.name, None, 0, "gauge")
    self.assert_(cdb)

    records = []
    start   = 1190860358

    for i in range(0, 20):
      records.append([ start+i, float(i) ])

    self.assertEqual(20, cdb.write_records(records))

    read = cdb.read_records(cooked = 1, step = 5)

    # for first 5, should have time of 1190860360 and value of 2
    # for next  5, should have time of 1190860365 and value of 7
    self.assertEqual(4, len(read))

    self.assertEqual(1190860360, read[0][0])
    self.assertEqual(2, read[0][1])

    self.assertEqual(1190860365, read[1][0])
    self.assertEqual(7, read[1][1])

    self.assertEqual(1190860370, read[2][0])
    self.assertEqual(12, read[2][1])

    self.assertEqual(1190860375, read[3][0])
    self.assertEqual(17, read[3][1])

    cdb.close()

if __name__ == '__main__':
  unittest.main()
