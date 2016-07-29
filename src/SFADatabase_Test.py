# Created on May 9, 2013
#
# @author: Ross Miller
# 
# Copyright 2013, 2015 UT Battelle, LLC
# 
# This work was supported by the Oak Ridge Leadership Computing Facility at
# the Oak Ridge National Laboratory, which is managed by UT Battelle, LLC for
# the U.S. DOE (under the contract No. DE-AC05-00OR22725).
# 
# This file is part of DDNTool_v2.
# 
# DDNTool_v2 is free software: you can redistribute it and/or modify it under
# the terms of the UT-Battelle Permissive Open Source License.  (See the
# License.pdf file for details.)
# 
# DDNTool_v2 is distributed in the hope that it will be useful, but WITHOUT ANY
# WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR
# A PARTICULAR PURPOSE.


# Note: these tests only work if there's a mysql server set up for us to write to...
# TODO: We don't have a db.verify_main_table() function any more, which means this
# test will abort down at the first assertEqual.   I should fix this!

# Note 2:  this test is very much out of date!  It should probably be renamed to
# SFAMySqlDb_test.py, and I've had to comment out the testMainTable() function
# because it relies on functions that no longer exist.  This means this test
# really only checks for db initialization...

import unittest
from DDNToolSupport.SFAClientUtils.SFAMySqlDb import SFAMySqlDb

DB_NAME = 'test'
DB_HOST = 'localhost'
DB_USER = 'xmr'
DB_PASSWORD = ''

class Test(unittest.TestCase):


    def testDBInit(self):
        db = SFAMySqlDb( DB_USER, DB_PASSWORD, DB_HOST, DB_NAME, True)
        # that's basically it: if we can create the object without throwing an
        # exception, the test passes.
    
'''
    Commenting out this function because it's *WAY* out of date.
    SFAMySqlDb.verify_main_table() doesn't exist and hasn't for proably a
    couple of years...
    
     
    def testMainTable(self):
        # Make sure this test happens *after* testDBInit()
        db = SFAMySqlDb( DB_USER, DB_PASSWORD, DB_HOST, DB_NAME, True)
        fake_clients = ["first", "second", "third", "fourth"]
        self.assertEqual(len(fake_clients), db.verify_main_table(fake_clients)) # should do a bunch of inserts
        self.assertEqual(0, db.verify_main_table(fake_clients)) # same client list - should do 0 inserts
'''    
        
        


if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testDBInit']
    unittest.main()