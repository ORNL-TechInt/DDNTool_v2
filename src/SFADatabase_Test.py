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

import unittest
from SFAClientUtils.SFADatabase import SFADatabase

DB_NAME = 'test'
DB_HOST = 'localhost'
DB_USER = 'xmr'
DB_PASSWORD = ''

class Test(unittest.TestCase):


    def testDBInit(self):
        db = SFADatabase( DB_USER, DB_PASSWORD, DB_HOST, DB_NAME, True)
        # that's basically it: if we can create the object without throwing an
        # exception, the test passes.
    
    def testMainTable(self):
        # Make sure this test happens *after* testDBInit()
        db = SFADatabase( DB_USER, DB_PASSWORD, DB_HOST, DB_NAME, True)
        fake_clients = ["first", "second", "third", "fourth"]
        self.assertEqual(len(fake_clients), db.verify_main_table(fake_clients)) # should do a bunch of inserts
        self.assertEqual(0, db.verify_main_table(fake_clients)) # same client list - should do 0 inserts
        
        
        


if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testDBInit']
    unittest.main()