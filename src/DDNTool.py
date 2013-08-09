#!/usr/bin/python
'''
Created on Mar 22, 2013

This will eventually become the new DDN monitoring tool. (For SFA hardware only.  The old DDNTool is still
needed for the S2A hardware.)

@author: xmr
'''

import ConfigParser
import argparse
import multiprocessing
import logging
import logging.handlers

from SFAClientUtils import SFAClient, SFADatabase

from bracket_expand import bracket_expand, bracket_aware_split

###### Remote Debugging using winpdb #######
#import rpdb2
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
    try:
        client = SFAClient.SFAClient( host, conf_file)
        client.run()
        # run() never returns
    except KeyboardInterrupt:
        # Perfectly normal.  Ctrl-C is how we expect to exit
        pass
    finally:
        logger = logging.getLogger( "DDNTool")
        logger.info( "Process %s is exiting.", host)
        print "Process ", host, " is exiting."

    
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
    parser.add_argument('-c', '--console_log',
                        help='Log to std err.  (Default is to use syslog)',
                        action='store_true')
    parser.add_argument( '-d', '--debug',
                         help="Include debug messages in the log",
                         action='store_true')

    main_args = parser.parse_args()

    config = ConfigParser.ConfigParser()
    config.read(main_args.conf_file)
    
    try:
        # Set up logging
        root_logger = logging.getLogger()
        if main_args.console_log:
            console_handler = logging.StreamHandler()
            # Add a timestamp to logs sent to std err (syslog automatically adds
            # its own timestamp, so we don't need to include ours in that case)
            
            formatter = logging.Formatter('%(asctime)s - %(name)s: - %(levelname)s - %(message)s')
            console_handler.setFormatter( formatter)
            root_logger.addHandler( console_handler)
        else:
            syslog_handler = logging.handlers.SysLogHandler('/dev/log')
            formatter = logging.Formatter('%(name)s: - %(levelname)s - %(message)s')
            syslog_handler.setFormatter( formatter)
            root_logger.addHandler( syslog_handler)

        if main_args.debug:
            root_logger.setLevel( logging.DEBUG)
        else:
            root_logger.setLevel( logging.INFO)
            
            # Disable some of the more excessive log messages that
            # the DDN API and its supporting libraries emit
            for log_name in [ 'APIContext', 'pywebm', 'root',
                              'SFADiskDriveStatistics', 'SFAPresentation',
                              'SFAVirtualDiskStatistics' ]:
                temp_log = logging.getLogger( log_name)
                temp_log.setLevel( logging.WARNING)

        logger = logging.getLogger( "DDNTool")
        
        # Initialize the database if requested
        if  main_args.init_db:
            logger.info(  "Initializing the the database...")
            print "Initializing the the database..."
            logger.debug( "Initializing the the database...")
            db_user = config.get('database', 'db_user')
            db_password = config.get('database', 'db_password')
            db_host = config.get('database', 'db_host')
            db_name = config.get('database', 'db_name')
            # don't actually need the db connection, but this is how we force
            # the db init code to run
            db = SFADatabase.SFADatabase(db_user, db_password, db_host, db_name, main_args.init_db)  # @UnusedVariable
            db = None  # @UnusedVariable

        # Fork a process for each controller in the config file
        sfa_processes = [] # holds the process objects, not SFAClient objects!
        sfa_hosts = [ host.strip() for host in
                bracket_aware_split(config.get('ddn_hardware', 'sfa_hosts')) ]
        bracket_expand( sfa_hosts)
        for host in sfa_hosts:
            logger.debug( "Creating process for host '%s'"%host)
            p = multiprocessing.Process(name='DDNTool_' + host, target=one_controller,
                                        args=(host, main_args.conf_file))
            p.daemon = False
            sfa_processes.append(p)
            logger.info("Starting background process for %s", host)
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
        logger.info( "Shutting down DDNTool")
        print "Shutting down DDNTool"
        
############ End of main_func() #####################

if __name__ == '__main__':
    main_func()
