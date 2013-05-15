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



# list of SFAClient objects.  Have to make this global so that
# the shutdown function can access it to shut the threads down
sfa_clients=[]

class Thread_Shutdown_Exception(Exception):
    '''
    An exception for indicating a problem shutting
    down a background thread.
    '''
    pass # Don't need anything other than what's in the base class

def clean_shutdown():
    '''
    Shut down the background thread(s) cleanly

    Returns nothing, but does not return until the threads have stopped.
    Raises a Thread_Shutdown_Exception exception if a thread fails
    to stop in a reasonable amount of time.
    '''

    # request each client to stop (using the option to
    # return immediately so we can get all the requests
    # in quickly
    for c in sfa_clients:
        c.stop_thread( False)

    # Send the thread stop request again, this time waiting
    # (hopefully not long) for the threads to exit
    for c in sfa_clients:
        if c.stop_thread( True, 10) == False:
            raise Thread_Shutdown_Exception, \
                "Failed to stop background thread for client %s"%c.get_host_name()


########## End of clean_shutdown() ##########################



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

 
    config = ConfigParser.ConfigParser()
    config.read(args.conf_file)
    
    try:
        # Connect to the DDN hardware
        sfa_user = config.get('ddn_hardware', 'sfa_user')
        sfa_password = config.get('ddn_hardware', 'sfa_password')
        sfa_hosts = [ host.strip() for host in config.get('ddn_hardware', 'sfa_hosts').split(",") ]
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

        # install the signal handler
        #signal.signal( signal.SIGINT, sigint_handler)

        # loop forever polling the clients and updating the database
        print "Entering main loop.  Ctrl-C to exit."
        while True:
            time.sleep(5) 
            for client in sfa_clients:
                read_iops = client.get_read_iops( 0, 5)
                write_iops = client.get_write_iops( 0, 5)
                bandwidth = client.get_transfer_bw( 0, 5)
                db.update_main_table(client.get_host_name(), bandwidth, read_iops, write_iops, 0, 0)
    except KeyboardInterrupt:
        # Perfectly normal.  Ctrl-C is how we expect to exit
        pass
    finally:
        print "Shutting down background threads."
        clean_shutdown()
        
############ End of main_func() #####################

if __name__ == '__main__':
    main_func()
