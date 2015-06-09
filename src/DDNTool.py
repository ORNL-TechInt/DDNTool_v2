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
import time

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


class ProcessData:
    '''
    Holds a few things we need to keep track of for each process: the process
    object itself and an Event that the process will wait on
    '''
    def __init__(self, p, e):
        self.p = p # process object
        self.e = e # event object

       
# event is a multiprocessing.Event object.
# update_time is a multiprocessing.Value object
def one_controller(host, conf_file, event, update_time):
    '''
    This is the function that gets called in a separate process.  It handles
    the polling and database updating for a single controller.
    '''
    logger = logging.getLogger( "DDNTool")
      
    client = SFAClient.SFAClient( host, conf_file, event, update_time)
    client.run()
    # run() loops until the main process sets update_time to 0

    logger.info( "Process %s is exiting.", host)
    print "Process ", host, " is exiting."


# proc_list is a list of ProcessData objects
# wake_time is how often the sub-processes should wake (in seconds)
# update_time is shared_mem object (multiprocessing.Value) that all the 
# sub-processes will use for their LastUpdate fields
def main_loop( proc_list, wake_time, update_time):
    '''
    Called by main_func() after the initialization has been completed.  Its
    job is to wake up all the processes at set intervals.
    
    Note: this function loops forever.  Ctrl-C is how we expect the user to
    break out of it.
    '''

    logger = logging.getLogger( "DDNTool")
    
    try:
        last_wake = time.time()

        while True:
            while (time.time() < last_wake + wake_time):
                time.sleep( 0.01)  # 10 millisec sleep
        
            # Wake up all the sub processes
            last_wake = time.time()
            update_time.value = int(last_wake)
            logger.debug( "Waking all sub-processes")
            for p in proc_list:
                p.e.set()  # set the event that each process is waiting on
                
            # When the processes have finished one iteration of their loops,
            # they will clear their events.  We wait for this so that we're
            # sure no subprocess is falling behind
            for p in proc_list:
                while p.e.is_set():
                    time.sleep( 0.01)
            logger.debug( "All sub-processes have completed their iterations")
                     
    except KeyboardInterrupt:
        # Perfectly normal.  Ctrl-C is how we expect to exit
        logger.debug( "Exiting from main loop")
    
    
def shutdown_subprocs( sfa_processes, update_time):
    '''
    Sends a 0 update time to all the subprocesses (which causes them to shut
    down) and waits for them to end.
    '''
    
            

    

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

    # shared memory value that all the sub-processes will have access to
    # main_loop() will update it with the time the sub-processes will use
    # for their LastUpdate fields
    update_time = multiprocessing.Value( 'L', 0)
    
    # Fork a process for each controller in the config file
    sfa_processes = [] # holds the ProcessData objects, not SFAClient objects!
    sfa_hosts = [ host.strip() for host in
            bracket_aware_split(config.get('ddn_hardware', 'sfa_hosts')) ]
    bracket_expand( sfa_hosts)
    for host in sfa_hosts:
        logger.debug( "Creating process for host '%s'"%host)
        e = multiprocessing.Event()
        e.clear()
        p = multiprocessing.Process(name='DDNTool_' + host, target=one_controller,
                                    args=(host, main_args.conf_file, e, update_time))
        p.daemon = False
        sfa_processes.append( ProcessData( p, e))
        
        logger.info("Starting background process for %s", host)
        print "Starting background process for", host
        p.start()
        
        
    # All processes are started (and are waiting on their events). Have
    # the main loop take over...
    wake_time = config.getfloat('polling', 'fast_poll_interval')
    main_loop( sfa_processes, wake_time, update_time)
    # if we've returned from main_loop(), it's because someone hit CTRL-C
    
    # Make sure all the events have been cleared by the sub processes
    # (Prevents a race condition caused by the fact that each sub-process
    # clears its event at the bottom of its loop.)
    for p in sfa_processes:
        if p.e.is_set():
            time.sleep( 0.01)  
    
    # Shut down all the sub-procs and exit
    update_time.value = 0   # Subprocs interpret a 0 update time as a
                            # shutdown command
    for p in sfa_processes:
        p.e.set()
        
    for p in sfa_processes:
        p.p.join()

    
    logger.info( "DDNTool exiting")
    print "DDNTool exiting"
        
############ End of main_func() #####################

if __name__ == '__main__':
    main_func()
