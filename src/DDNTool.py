'''
Created on Mar 22, 2013

This will eventually become the new DDN monitoring tool. (For SFA hardware only.  The old DDNTool is still
needed for the S2A hardware.)

@author: xmr
'''

import ConfigParser
import time
import argparse
import multiprocessing

import SFAClient
import SFADatabase

from SFATimeSeries import EmptyTimeSeriesException

###### Remote Debugging using winpdb #######
import rpdb2
#rpdb2.start_embedded_debugger('xmr')
# xmr is the session password - make sure port 51000 is open
# Note: calling stat_embedded_debuger will cause the program execution to
# freeze until the debugger actually connects to it.
#############################################

DEFAULT_CONF_FILE="./ddntool.conf"  # config file to use if not specified on the command line 


def one_controller(host, conf_file):
    '''
    This is the function that gets called in a separate process.  It handles
    the polling and database updating for a single controller.
    '''
    client = SFAClient.SFAClient( host, conf_file)
    client.run()
    # run() never returns
    
    
    
     

def main_func():
    
    # Quick summary:
    # Parse command line args
    # Open & parse the config file
    # If requested, open the DB and init the tables
    # Fork off a process for each controller in the config file
    
    

    parser = argparse.ArgumentParser()
    parser.add_argument("-f", "--conf_file",
                        help="Specify the name of the configuration file.  (Default is '" + 
                             DEFAULT_CONF_FILE + "')",
                        default=DEFAULT_CONF_FILE)
    parser.add_argument('-i', '--init_db',
                        help='Initialize the database on startup.',
                        action='store_true');

    main_args = parser.parse_args()

    config = ConfigParser.ConfigParser()
    config.read(main_args.conf_file)
    
    try:
        
        # Initialize the database if requested
        if  main_args.init_db:
            print "Initializing the the database..."
            db_user = config.get('database', 'db_user')
            db_password = config.get('database', 'db_password')
            db_host = config.get('database', 'db_host')
            db_name = config.get('database', 'db_name')
            db = SFADatabase.SFADatabase(db_user, db_password, db_host, db_name, main_args.init_db)
            db = None  # don't actually need the db connection.  Just wanted to run the init code


        # Fork a process for each controller in the config file
        sfa_processes = [] # holds the process objects, not SFAClient objects!
        sfa_hosts = [ host.strip() for host in config.get('ddn_hardware', 'sfa_hosts').split(",") ]
        for host in sfa_hosts:
            p = multiprocessing.Process(name='DDNTool_' + host, target=one_controller,
                                        args=(host, main_args.conf_file))
            p.daemon = False
            sfa_processes.append(p)
            print "Starting background process for", host
            p.start()
            
 
 
        # all the real work is done in the background processes, so we're
        # just going to sit here and wait
        for p in sfa_processes:
            p.join()
            
    except KeyboardInterrupt:
        # Perfectly normal.  Ctrl-C is how we expect to exit
        pass
    finally:
        print "Shutting down DDNTool"
        # the multiprocessing library should automatically kill all the
        # background processes when the main process exits.  That's why
        # we set the daemon flag to False above.

        
############ End of main_func() #####################

if __name__ == '__main__':
    main_func()
