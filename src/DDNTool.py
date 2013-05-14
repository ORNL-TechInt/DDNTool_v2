'''
Created on Mar 22, 2013

This will eventually become the new DDN monitoring tool. (For SFA hardware only.  The old DDNTool is still
needed for the S2A hardware.)

@author: xmr
'''

import ConfigParser
import sys
import time
import argparse

import SFAClient
import SFADatabase

###### Remote Debugging using winpdb #######
import rpdb2
#rpdb2.start_embedded_debugger('xmr')
# xmr is the session password - make sure port 51000 is open
# Note: calling stat_embedded_debuger will cause the program execution to
# freeze until the debugger actually connects to it.
#############################################

DEFAULT_CONF_FILE="./ddntool.conf"  # config file to use if not specified on the command line 

def main_func():
    
    # Quick summary:
    # Open & parse the config file
    # Open the DB and create the in-memory tables
    # Create SFAClient instances for each controller (possibly just each controller pair??)
    # Start up the main loop:
    #     poll each SFAClient
    #     push the results up to the DB
    

    parser = argparse.ArgumentParser()
    parser.add_argument("-f", "--conf_file",
                        help="Specify the name of the configuration file.  (Default is '" + 
                             DEFAULT_CONF_FILE + "')",
                        default=DEFAULT_CONF_FILE)
    parser.add_argument('-i', '--init_db',
                        help='Initialize the database on startup.',
                        action='store_true');

    args = parser.parse_args()


    print "Args: ",
    print args
    
    
    
    config = ConfigParser.ConfigParser()
    config.read(args.conf_file)
    
    # Connect to the DDN hardware
    sfa_user = config.get('ddn_hardware', 'sfa_user')
    sfa_password = config.get('ddn_hardware', 'sfa_password')
    sfa_hosts = [ host.strip() for host in config.get('ddn_hardware', 'sfa_hosts').split(",") ]
    sfa_clients = []
    for host in sfa_hosts:
        client = SFAClient.SFAClient( host, sfa_user, sfa_password)
        sfa_clients.append( client)

    time.sleep(5)
    if not client.is_connected():
        print "Failed to connect to SFA Host %s"%host
     
    # Connect to the database
    db_user = config.get('database', 'db_user')
    db_password = config.get('database', 'db_password')
    db_host = config.get('database', 'db_host')
    db_name = config.get('database', 'db_name')
    db = SFADatabase.SFADatabase(db_user, db_password, db_host, db_name, args.init_db)
    db.verify_main_table( sfa_hosts)
    
    time.sleep(10) # give the background thread some time to poll a couple of times
    for client in sfa_clients:
        read_iops = client.get_read_iops( 0, 5)
        db.update_main_table(client.get_host_name(), 3.1415, 2.71, read_iops, 0, 0, 0)

        
    print "Stopping threads..."
    for c in sfa_clients:
        if c.stop_thread(True, 15):
            print "Thread ended"
        else:
            print "Thread still running"
    
    for c in sfa_clients:
        c.join()
        print "Thread definitely ended"


if __name__ == '__main__':
    main_func()
