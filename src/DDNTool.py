#!/usr/bin/python
'''
Created on Mar 22, 2013

This will eventually become the new DDN monitoring tool. (For SFA hardware only.  The old DDNTool is still
needed for the S2A hardware.)

@author: Ross Miller
'''

import ConfigParser
import argparse
import multiprocessing
import logging
import os
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

logger = None  # logging object at global (module) scope so everyone can use it
               # Initialized down in main_func()

class ProcessData:
    '''
    Holds a few things we need to keep track of for each process: the process
    object itself and an Event that the process will wait on
    '''
    def __init__(self, host, conf_file, update_time):
        '''
        Create an event and a process, then start the process.
        
        
        host is a string with the hostname
        conf_file is a string with the name of the config file
        update_time is a shared memory value (Multiprocessing.Value) object
        that the processes will use to get their update time values.
        '''
        
        self.host=host
        self.conf_file=conf_file
        self.update_time=update_time
        
        self.restart()
        
    def restart( self):
        '''Restart the process'''
                
        self.e = multiprocessing.Event()
        self.e.clear()
        
        proc_name = 'DDNTool_' + self.host
        logger.debug( "Creating process for host '%s'"%self.host)
        self.p = multiprocessing.Process(name=proc_name,
                                         target=one_controller,
                                         args=(self.host, self.conf_file, 
                                               self.e, self.update_time))
        self.p.daemon = False
        logger.info("Starting background process for %s", self.host)
        print "Starting background process for", self.host
        self.p.start()
    
    def is_alive(self):
        '''
        Check to see if the process is still alive
        
        The Process object has its own is_alive() function, but it doesn't
        seem to work at all.  So, this function is based around the
        os.waitpid() function.  It's called with the WNOHANG option, so it
        will always return immediately.
        
        If the process is running normally, waitpid() returns (0, 0).
        If the process happens to be a zombie,waitpid() cleans up the resources
        and allows it to exit.  Then it returns something other than (0, 0).
        If the process is completely gone, waitpid() throws an OSError.
        '''
        process_dead = False
        try:
            if os.waitpid( self.p.pid, os.WNOHANG) != (0, 0):
                process_dead = True
        except OSError:
            process_dead = True
            
        if process_dead:
            # Do some cleanup work: If the process has exited, then the event
            # is going to be buggered as well.  Best thing to do is create a
            # new one.  Even if we don't start a replacement process, at least
            # calls to e.set() will continue to work.
            self.e = multiprocessing.Event()
            self.e.clear()
            
        return not process_dead
    
       
# event is a multiprocessing.Event object.
# update_time is a multiprocessing.Value object
def one_controller(host, conf_file, event, update_time):
    '''
    This is the function that gets called in a separate process.  It handles
    the polling and database updating for a single controller.
    '''
    logger = logging.getLogger( "DDNTool")
    
    try:
        client = SFAClient.SFAClient( host, conf_file, event, update_time)
        client.run()
        # run() loops until the main process sets update_time to 0
    except Exception, e:
        logger.error( "Process %s caught %s exception."%(host,
                                                         type(e).__name__))

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
        
            # Make sure all the sub processes are still alive
            for p in proc_list:
                if not p.is_alive():
                    logger.error( "Process %s has crashed!  Restarting!"%p.p.name)
                    p.restart()
                    
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
                while p.is_alive() and p.e.is_set():
                    time.sleep( 0.01)
            logger.debug( "All sub-processes have completed their iterations")
            logger.debug("") # Insert a blank line in the debug log - makes it
                             # easier to figure out where the loop iteration
                             # stops
                     
    except KeyboardInterrupt:
        # Perfectly normal.  Ctrl-C is how we expect to exit
        logger.debug( "Exiting from main loop")
      

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

    global logger
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
        sfa_processes.append( ProcessData( host, main_args.conf_file, update_time))       
        
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
