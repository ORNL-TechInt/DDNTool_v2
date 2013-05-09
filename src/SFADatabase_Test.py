'''
Created on May 9, 2013

@author: xmr
'''

# Note: these tests only work if there's a mysql server set up for us to write to...
import unittest
from SFADatabase import SFADatabase

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