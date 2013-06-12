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

from SFATimeSeries import EmptyTimeSeriesException

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


def clean_shutdown():
    '''
    Shut down the background thread(s) cleanly

    Returns nothing, but does not normally return until the threads
    have stopped.  If the thread doesn't stop within the timeout time,
    it prints an error message.
    '''

    # request each client to stop (using the option to
    # return immediately so we can get all the requests
    # in quickly
    for c in sfa_clients:
        c.stop_thread( False)

    # Send the thread stop request again, this time waiting
    # (hopefully not long) for the threads to exit
    for c in sfa_clients:
        if c.stop_thread( True, 15) == False:
            print "Failed to stop background thread for client %s"%c.get_host_name()


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
        # Get the polling intervals from the config file
        fast_poll_interval = config.getfloat('polling', 'fast_poll_interval')
        med_poll_multiple = config.getint('polling', 'med_poll_multiple')
        slow_poll_multiple = config.getint('polling', 'slow_poll_multiple')

        # Connect to the DDN hardware
        sfa_user = config.get('ddn_hardware', 'sfa_user')
        sfa_password = config.get('ddn_hardware', 'sfa_password')
        sfa_hosts = [ host.strip() for host in config.get('ddn_hardware', 'sfa_hosts').split(",") ]
        for host in sfa_hosts:
            client = SFAClient.SFAClient( host, sfa_user, sfa_password)
            sfa_clients.append( client)

        # Wait for the hosts to connect and complete at least one
        # pass through their main loops
        print "Waiting for clients to connect and initialize"
        MAX_CLIENT_WAIT=20  # in seconds
        # Note:  Turn this *way* down once we get a faster API from DDN

        for client in sfa_clients:
            end_time = time.time() + MAX_CLIENT_WAIT
            while (not client.is_ready()) and (time.time() < end_time):
                time.sleep(0.25)
            if not client.is_ready():
                if not client.is_connected():
                    print "Failed to connect to SFA Host %s"%client.get_host_name()
                else:
                    print "Connected to SFA Host %s, could not complete initialization steps."%client.get_host_name()
                client.stop_thread( False)
                sfa_clients.remove( client)
            else:
                print "%s ready"%client.get_host_name()
    
        # Connect to the database
        print "Connecting to the database..."
        db_user = config.get('database', 'db_user')
        db_password = config.get('database', 'db_password')
        db_host = config.get('database', 'db_host')
        db_name = config.get('database', 'db_name')
        db = SFADatabase.SFADatabase(db_user, db_password, db_host, db_name, args.init_db)

        # loop forever polling the clients and updating the database
        print "Entering main loop.  Ctrl-C to exit."
        next_fast_poll_time = 0
        fast_iteration = -1

        while True:

            while (time.time() < next_fast_poll_time):
                time.sleep(fast_poll_interval / 10) # wait for the poll time to come up

            next_fast_poll_time = time.time() + fast_poll_interval
            fast_iteration += 1

            ############# Fast Interval Stuff #####################
            for client in sfa_clients:
                lun_nums = client.get_lun_nums()
                for lun_num in lun_nums:
                    try:
                        read_iops = client.get_time_series_average( 'lun_read_iops', lun_num, 60)
                        write_iops = client.get_time_series_average( 'lun_write_iops', lun_num, 60)
                        bandwidth = client.get_time_series_average( 'lun_transfer_bytes', lun_num, 60)
                        fw_bandwidth = client.get_time_series_average( 'lun_forwarded_bytes', lun_num, 60)
                        fw_iops = client.get_time_series_average( 'lun_forwarded_iops', lun_num, 60)
                        db.update_lun_table(client.get_host_name(), lun_num, bandwidth[0],
                                           read_iops[0], write_iops[0], fw_bandwidth[0],
                                           fw_iops[0])
                    except EmptyTimeSeriesException:
                        print "Skipping empty time series for host %s, virtual disk %d"% \
                                (client.get_host_name(), lun_num)


                dd_nums = client.get_dd_nums()
                for dd_num in dd_nums:
                    try:
                        read_iops = client.get_time_series_average( 'dd_read_iops', dd_num, 60)
                        write_iops = client.get_time_series_average( 'dd_write_iops', dd_num, 60)
                        bandwidth = client.get_time_series_average( 'dd_transfer_bytes', dd_num, 60)
                        db.update_dd_table(client.get_host_name(), dd_num, bandwidth[0],
                                           read_iops[0], write_iops[0])
                    except EmptyTimeSeriesException:
                        print "Skipping empty time series for host %s, disk drive %d"% \
                              (client.get_host_name(), dd_num)
           
            ############# Medium Interval Stuff #####################
            if (fast_iteration % med_poll_multiple == 0):
                for client in sfa_clients:
                    lun_nums = client.get_lun_nums()
                    for lun_num in lun_nums:
                        request_values = client.get_lun_io_read_request_sizes( lun_num)
                        db.update_lun_request_size_table( client.get_host_name(), lun_num, True, request_values)
                        request_values = client.get_lun_io_write_request_sizes( lun_num)
                        db.update_lun_request_size_table( client.get_host_name(), lun_num, False, request_values)
                        request_values = client.get_lun_io_read_latencies( lun_num)
                        db.update_lun_request_latency_table( client.get_host_name(), lun_num, True, request_values)
                        request_values = client.get_lun_io_write_latencies( lun_num)
                        db.update_lun_request_latency_table( client.get_host_name(), lun_num, False, request_values)

                    dd_nums = client.get_dd_nums()
                    for dd_num in dd_nums:
                        request_values = client.get_dd_io_read_request_sizes( dd_num)
                        db.update_dd_request_size_table( client.get_host_name(), dd_num, True, request_values)
                        request_values = client.get_dd_io_write_request_sizes( dd_num)
                        db.update_dd_request_size_table( client.get_host_name(), dd_num, False, request_values)
                        request_values = client.get_dd_io_read_latencies( dd_num)
                        db.update_dd_request_latency_table( client.get_host_name(), dd_num, True, request_values)
                        request_values = client.get_dd_io_write_latencies( dd_num)
                        db.update_dd_request_latency_table( client.get_host_name(), dd_num, False, request_values)



            ############# Slow Interval Stuff #######################
            if (fast_iteration % slow_poll_multiple == 0):
                # TODO: slow interval stuff
                pass
 
    except KeyboardInterrupt:
        # Perfectly normal.  Ctrl-C is how we expect to exit
        pass
    finally:
        print "Shutting down background threads."
        clean_shutdown()
        
############ End of main_func() #####################

if __name__ == '__main__':
    main_func()
