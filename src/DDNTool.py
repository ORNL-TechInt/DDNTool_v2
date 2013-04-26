'''
Created on Mar 22, 2013

This will eventually become the new DDN monitoring tool. (For SFA hardware only.  The old DDNTool is still
needed for the S2A hardware.)

@author: xmr
'''

import sys
import SFAClient

def main_func():

    # Quick summariy:
    # Open & parse the config file
    # Open the DB and create the in-memory tables
    # Create SFAClient instances for each controller (possibly just each controller pair??)
    # Start up the main loop:
    #     poll each SFAClient
    #     push the results up to the DB
    

    print "Args: ",
    for arg in sys.argv:
        print arg,
    
    print    
    print "Hello World!"
    
    c = SFAClient.SFAClient( "sultan-12k1", "xmr", "bogus_password")
        
    print "Stopping thread..."
    if c.stop_thread(True, 15):
        print "Thread ended"
    else:
        print "Thread still running"
    
    c.join()
    print "Thread definitely ended"



if __name__ == '__main__':
    main_func()