'''
Created on Mar 22, 2013

This will eventually become the new DDN monitoring tool. (For SFA hardware only.  The old DDNTool is still
needed for the S2A hardware.)

@author: xmr
'''

import ConfigParser
import sys
import time

import SFAClient
import SFADatabase

DEFAULT_CONF_FILE="./ddntool.conf"  # config file to use if not specified on the command line 

def main_func():

    # Quick summary:
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
    
    
    config = ConfigParser.ConfigParser()
    config.read(DEFAULT_CONF_FILE)
    
    # Connect to the DDN hardware
    #sfa_user = config.get('ddn_hardware', 'sfa_user')
    #sfa_password = config.get('ddn_hardware', 'sfa_password')
    #sfa_hosts = [ host.strip() for host in config.get('ddn_hardware', 'sfa_hosts').split(",") ]
    #sfa_clients = []
    #for host in sfa_hosts:
    #    client = SFAClient.SFAClient( host, sfa_user, sfa_password)
    #    if client.is_connected():
    #        sfa_clients.append()
    #    else:
    #        print "Failed to connect to SFA Host %s"%host
     
    # Connect to the database
    db_user = config.get('database', 'db_user')
    db_password = config.get('database', 'db_password')
    db_host = config.get('database', 'db_host')
    db_name = config.get('database', 'db_name')
    db = SFADatabase.SFADatabase(db_user, db_password, db_host, db_name)
    

    #time.sleep(10) # give the background thread some time to poll a couple of times
    #for i in range(5):
    #    print c.get_read_iops( 0, 5)
    #    time.sleep( 10)

        
    print "Stopping thread..."
    if c.stop_thread(True, 15):
        print "Thread ended"
    else:
        print "Thread still running"
    
    c.join()
    print "Thread definitely ended"



if __name__ == '__main__':
    main_func()