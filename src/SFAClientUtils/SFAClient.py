# Created on Mar 22, 2013
# 
# @author: Ross Miller
# 
# Copyright 2013, 2015 UT Battelle, LLC
# 
# This work was supported by the Oak Ridge Leadership Computing Facility at
# the Oak Ridge National Laboratory, which is managed by UT Battelle, LLC for
# the U.S. DOE (under the contract No. DE-AC05-00OR22725).
# 
# This file is part of DDNTool_v2.
# 
# DDNTool_v2 is free software: you can redistribute it and/or modify it under
# the terms of the UT-Battelle Permissive Open Source License.  (See the
# License.pdf file for details.)
# 
# DDNTool_v2 is distributed in the hope that it will be useful, but WITHOUT ANY
# WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR
# A PARTICULAR PURPOSE.


import ConfigParser
import logging
import SFADatabase
from SFATimeSeries import SFATimeSeries
from SFATimeSeries import EmptyTimeSeriesException

from ddn.sfa.api import *

#
# Note:  There are several code blocks that deal with the data from the 
# SFADiskDriveStatistics class.  This code has all be commented out because
# processing those objects is so slow.  I'm keeping the code around though
# in case we change our minds about this.
#

MINIMUM_FW_VER = '2.3.0' 
# 2.3.0 is needed for the read & write bandwidth numbers

class UnexpectedClientDataException( Exception):
    '''
    Used when the DDN API sent back data that we weren't expecting
    or don't understand.  This is sort of one step up from an
    assert.  Hopefully, we won't use it too often.
    '''
    pass    # don't need anything besides what's already in the base class

    
class SFAClient():
    '''
    A class that represents a client for the SFA API.  Specifically, each instance will connect to a DDN
    SFA controller and poll it repeatedly for certain data.  The instance will format and pre-process
    the data and then push it up to the database specified in the config file.
    
    This class is designed to be used from its own process via the multiprocessing library.  The
    only "public" function it has is run().
    '''

    def __init__(self, address, conf_file, event, update_time):
        '''
        Constructor
        '''

        # Get the logger object
        self.logger = logging.getLogger( 'DDNTool_SFAClient_%s'%address)
        self.logger.debug( 'Creating instance of SFAClient')
                
        # parameters for accessing the SFA hardware       
        self._address = address 
        self._uri = "https://" + address
        # user and password are in the config file.  (So's the address, but
        # *all* the addresses are in there and we wouldn't know which one to
        # connect to.)        
        
        self._connected = False;
        self._exit_requested = False;
        
        # open up the config file and grab settings for the database and
        # polling intervals
        self.logger.debug( 'Parsing config file')
        self._parse_config_file( conf_file)
        
        # Time series data
        # This is a dictionary of dictionaries of SFATimeSeries objects
        # The outer dictionary maps the type of data (ie: vd_read_iops),
        # the inner dictionary maps the actual device number.
        # (We use an inner dictionary instead of a list so the device
        # numbers don't have to be sequential.)
        self._time_series = {}
  
        # Statistics objects
        # We keep copies of each SFAVirtualDiskStatistics and 
        # SFADiskDriveStatistics object (mainly for the I/O latency and
        # request size arrays).
        # Note: _vd_stats is indexed by the LUN number.  _dd_stats is
        # indexed by the disk drive number
        self._vd_stats = {}
#        self._dd_stats = {}
        
        # Storage pool state
        # We currently keep only one field from the SFAStoragePool classes: PoolState
        # The dictionary is indexed by the LUN number of the LUN that is built from the pool.
        self._storage_pool_states = {}

        # LUN to virtual disk map
        # The statistics objects deal with virtual disks, but we want to display
        # everything as LUN's.  This maps one to the other.  (VD index is the key,
        # LUN number is the value.) It's updated at the medium frequency.
        self._vd_to_lun = { }

        # open a connection to the database
        self.logger.debug( 'Opening DB connection')
        self._db = SFADatabase.SFADatabase(self._db_user, self._db_password,
                                           self._db_host, self._db_name, False)
        
        # connect to the SFA controller
        self.logger.debug( 'Connecting to DDN hardware')
        APIConnect( self._uri, (self._sfa_user, self._sfa_password))        # @UndefinedVariable
        self.logger.debug( 'Connection established.')

        self.logger.debug( 'Calling _time_series_init()')
        self._time_series_init()
        self.logger.debug( '_time_series_init() completed.  Calling _check_labels()')
        self._check_labels()    # verify the labels for the request sizes and latencies
                                # match what we've hard-coded into the database
                                
        # Save the event and update time object
        # event is a multiprocessing.Event object and update_time is a
        # multiprocessing.Value object
        self._event = event
        self._update_time = update_time
        
        self.logger.debug( '__init__ completed')
        
        
    def run(self):
        '''
        Main loop: Waits on the event, then polls the SFA, post-processes the
        data, publishes it to the database.  Then it clears the event.  Runs
        until the main processes sends an update time of 0.
        '''
        
        # make sure the firmware is new enough to have the features we need
        self.logger.debug( 'Verifying Controller Firmware Version')
        if not self._verify_fw_version():
            return  # _verify_fw_version will output the necessary lines to the log        
        
        self.logger.debug( 'Starting main loop')
        
        # Run the fast poll stuff once right away.  The reason has to do with the time
        # series data:  in order to calculate an average, we need 2 data points.  Calling
        # the fast poll tasks now loads the first data point in all the series.  The second
        # point will be added down in the main loop when the _fast_poll_tasks() is called
        # again.  This means that by the time we get down to the db update code, all the
        # time series should be able to return a value for their average and we shouldn't
        # get any EmptyTimeSeries exceptions.
        self._fast_poll_tasks()

        fast_iteration = -1 # This is initialized to -1 in order to force us to execute
                            # the medium and slow poll stuff the first time we pass
                            # through the while loop.
        
        while not self._exit_requested:  # loop until we're told not to
            
            self.logger.debug( "Waiting on event")
            self._event.wait()  # wait until we're told to poll
            self.logger.debug( "Waking up")
                      
            fast_iteration += 1
            
            # Check if we're supposed to exit
            if self._update_time.value == 0:
                self._exit_requested = True
                break
                
            
            ############# Fast Interval Stuff #######################
            self._fast_poll_tasks()           
            
            ############# Medium Interval Stuff #####################
            if (fast_iteration % self._med_poll_multiple == 0):
                self._medium_poll_tasks()
            
            ############# Slow Interval Stuff #######################
            if (fast_iteration % self._slow_poll_multiple == 0):
                self._slow_poll_tasks()

            ##=====================Database Stuff====================
            # Note: the database operations are down here after the polling operations
            # to ensure that everything is polled at least once before we try to push
            # anything to the database
            ############# Fast Interval Stuff #######################
            self._fast_database_tasks()
                        
            ############# Medium Interval Stuff #####################
            if (fast_iteration % self._med_poll_multiple == 0):
                self.logger.debug( 'Executing medium rate DB tasks')
                self._medium_database_tasks()
            
            ############# Slow Interval Stuff #######################
            if (fast_iteration % self._slow_poll_multiple == 0):
                self.logger.debug( 'Executing slow rate DB tasks')
                self._slow_database_tasks()
                        
            self._event.clear();    # Clear the event to signal that we're done
                                    # processing this iteration
        # end of main while loop
    # end of run() 


    def _fast_poll_tasks(self):
        '''
        Retrieves all the values we need to get from the controller at the fast interval.
        '''
        ##Virtual Disk Statistics 
        vd_stats = SFAVirtualDiskStatistics.getAll()  # @UndefinedVariable
        
        self._vd_stats = { } # erase the old _vd_stats dictionary
        for stats in vd_stats:
            index = stats.Index

            # Save the entire object (mainly for its I/O latency and request
            # size arrays
            self._vd_stats[self._vd_to_lun[index]] = stats
            
            # Note: we actually get back 2 element lists - one element
            # for each controller in the couplet.  In theory, one of those
            # elements should always be 0.
            self._time_series['lun_read_iops'][self._vd_to_lun[index]].append(stats.ReadIOs[0] + stats.ReadIOs[1])
            self._time_series['lun_write_iops'][self._vd_to_lun[index]].append(stats.WriteIOs[0] + stats.WriteIOs[1])
            self._time_series['lun_transfer_bytes'][self._vd_to_lun[index]].append(
                    (stats.KBytesTransferred[0] + stats.KBytesTransferred[1]) * 1024)
            # Note: converted to bytes
            
            self._time_series['lun_read_bytes'][self._vd_to_lun[index]].append(
                    (stats.KBytesRead[0] + stats.KBytesRead[1]) * 1024)
            # Note: converted to bytes
            
            self._time_series['lun_write_bytes'][self._vd_to_lun[index]].append(
                    (stats.KBytesWritten[0] + stats.KBytesWritten[1]) * 1024)
            # Note: converted to bytes

            self._time_series['lun_forwarded_bytes'][self._vd_to_lun[index]].append(
                    (stats.KBytesForwarded[0] + stats.KBytesForwarded[1]) * 1024)
            # Note: converted to bytes 

            self._time_series['lun_forwarded_iops'][self._vd_to_lun[index]].append(
                    stats.ForwardedIOs[0] + stats.ForwardedIOs[1])

        ##Disk Statistics
# Disabling this code because we don't need it at the fast rate.
#        disk_stats = SFADiskDriveStatistics.getAll()
#        for stats in disk_stats:
#            index = stats.Index
#
#            # Note: we actually get back 2 element lists - one element
#            # for each controller in the couplet.  In theory, one of those
#            # elements should always be 0.
#            self._time_series['dd_read_iops'][index].append(stats.ReadIOs[0] + stats.ReadIOs[1])
#            self._time_series['dd_write_iops'][index].append(stats.WriteIOs[0] + stats.WriteIOs[1])
#            self._time_series['dd_transfer_bytes'][index].append(
#                    (stats.KBytesTransferred[0] + stats.KBytesTransferred[1]) * 1024)
            # Note: converted to bytes


    def _medium_poll_tasks(self):
        '''
        Retrieves all the values we need to get from the controller at the medium interval.
        ''' 
        # Update the LUN to virtual disk map.  (We probably don't
        # need to do this very often, but it's not a lot of work
        # and this way if an admin ever makes any changes, they'll
        # show up fairly quickly
        self._update_lun_map()
        
        # Grab the storage pool data (so we can find out if the pool is in a degraded state)
        # Store it in a temporary dictionary, indexed by the pool's Index member
        storage_pools = SFAStoragePool.getAll()  # @UndefinedVariable
        pools_d = { }
        for pool in storage_pools:
            pools_d[pool.Index] = pool
        
        self._storage_pool_states = { } # erase the old _storage_pool_states dictionary    

        # Now, get all the virtual disks and map them back to the pool they're created
        # from.  (For now, we just want the pool state, not the whole SFAStoragePool object)
        virt_disks = SFAVirtualDisk.getAll()  # @UndefinedVariable
        for disk in virt_disks:
            # Save the PoolState field in the dictionary
            self._storage_pool_states[self._vd_to_lun[disk.Index]] = pools_d[disk.PoolIndex].PoolState
        

        


    def _slow_poll_tasks(self):
        '''
        Retrieves all the values we need to get from the controller at the fast interval.
        '''
        pass # no slow poll tasks yet

    
    def _fast_database_tasks(self):
        '''
        Update all the values in the database that need to be updated at the fast rate.
        '''

        for lun_num in self._vd_to_lun.values():
            try:
                read_iops = self._get_time_series_average( 'lun_read_iops', lun_num, 60)
                write_iops = self._get_time_series_average( 'lun_write_iops', lun_num, 60)
                transfer_bandwidth = self._get_time_series_average( 'lun_transfer_bytes', lun_num, 60)
                read_bandwidth = self._get_time_series_average( 'lun_read_bytes', lun_num, 60)
                write_bandwidth = self._get_time_series_average( 'lun_write_bytes', lun_num, 60)
                fw_bandwidth = self._get_time_series_average( 'lun_forwarded_bytes', lun_num, 60)
                fw_iops = self._get_time_series_average( 'lun_forwarded_iops', lun_num, 60)
                
                # Get the pool state we copied out of the associated SFAStoragePool object
                # Note: this object is only updated at the medium rate
                try:
                    pool_state = self._storage_pool_states[lun_num]
                except KeyError:
                    self.logger.error( "No storage pool states mapped to LUN number %d!!"%lun_num)
                    self.logger.error( "Setting pool state to UNKNOWN!")
                    pool_state = 255
                
                self._db.update_lun_table(self._get_host_name(), self._update_time.value, 
                                          lun_num, transfer_bandwidth[0],
                                          read_bandwidth[0], write_bandwidth[0],
                                          read_iops[0], write_iops[0],
                                          fw_bandwidth[0], fw_iops[0], pool_state)
            
            except EmptyTimeSeriesException:
                print "Skipping empty time series for host %s, virtual disk %d"% \
                        (self._get_host_name(), lun_num)
                   
            # Work on the values for the raw lun table (grab the raw
            # values out of the saved stats object)
            tmp_stats = self._vd_stats[lun_num]
            
            read_bytes = (tmp_stats.KBytesRead[0] + tmp_stats.KBytesRead[1]) * 1024
            write_bytes = (tmp_stats.KBytesWritten[0] + tmp_stats.KBytesWritten[1]) * 1024
            transfer_bytes = (tmp_stats.KBytesTransferred[0] + tmp_stats.KBytesTransferred[1]) * 1024
            forwarded_bytes = (tmp_stats.KBytesForwarded[0] + tmp_stats.KBytesForwarded[1]) * 1024
            # Note: converted to bytes
            
            total_ios = (tmp_stats.TotalIOs[0] + tmp_stats.TotalIOs[1])
            forwarded_ios = (tmp_stats.ForwardedIOs[0] + tmp_stats.ForwardedIOs[1])
            read_ios = (tmp_stats.ReadIOs[0] + tmp_stats.ReadIOs[1])
            write_ios = (tmp_stats.WriteIOs[0] + tmp_stats.WriteIOs[1])
            
            self._db.update_raw_lun_table( self._get_host_name(), self._update_time.value,
                          lun_num, transfer_bytes,read_bytes, write_bytes,
                          forwarded_bytes, total_ios, read_ios, write_ios,
                          forwarded_ios, pool_state)
                



# It turns out that we don't care about the per-disk iops & bandwidth
#        for dd_num in self._dd_stats.keys():
#            try:
#                read_iops = self._get_time_series_average( 'dd_read_iops', dd_num, 60)
#                write_iops = self._get_time_series_average( 'dd_write_iops', dd_num, 60)
#                bandwidth = self._get_time_series_average( 'dd_transfer_bytes', dd_num, 60)
#                self._db.update_dd_table(self._get_host_name(), self._update_time.value,
#                                   dd_num, bandwidth[0],
#                                   read_iops[0], write_iops[0])
#            except EmptyTimeSeriesException:
#                print "Skipping empty time series for host %s, disk drive %d"% \
#                      (self._get_host_name(), dd_num)


    def _medium_database_tasks(self):
        '''
        Update all the values in the database that need to be updated at the medium rate.
        '''
        for lun_num in self._vd_to_lun.values():
            request_values =  self._vd_stats[lun_num].ReadIOSizeBuckets
            self._db.update_lun_request_size_table( self._get_host_name(),
                    self._update_time.value, lun_num, True, request_values)
            request_values =  self._vd_stats[lun_num].WriteIOSizeBuckets
            self._db.update_lun_request_size_table( self._get_host_name(),
                    self._update_time.value, lun_num, False, request_values)
            request_values =  self._vd_stats[lun_num].ReadIOLatencyBuckets
            self._db.update_lun_request_latency_table( self._get_host_name(),
                    self._update_time.value, lun_num, True, request_values)
            request_values =  self._vd_stats[lun_num].WriteIOLatencyBuckets
            self._db.update_lun_request_latency_table( self._get_host_name(),
                    self._update_time.value, lun_num, False, request_values)

#        for dd_num in self._dd_stats.keys():
#            request_values = self._dd_stats[dd_num].ReadIOSizeBuckets
#            self._db.update_dd_request_size_table( self._get_host_name(),
#                    self._update_time.value, dd_num, True, request_values)
#            request_values = self._dd_stats[dd_num].WriteIOSizeBuckets
#            self._db.update_dd_request_size_table( self._get_host_name(),
#                    self._update_time.value, dd_num, False, request_values)
#            request_values = self._dd_stats[dd_num].ReadIOLatencyBuckets
#            self._db.update_dd_request_latency_table( self._get_host_name(),
#                    self._update_time.value, dd_num, True, request_values)
#            request_values = self._dd_stats[dd_num].WriteIOLatencyBuckets
#            self._db.update_dd_request_latency_table( self._get_host_name(),
#                    self._update_time.value, dd_num, False, request_values)

        
    def _slow_database_tasks(self):
        '''
        Update all the values in the database that need to be updated at the slow rate.
        '''
        pass  # no slow tasks yet


    def _parse_config_file(self, conf_file):
        '''
        Opens up the specified config file and reads settings for SFA & database
        access and polling intervals.
        '''
         
        config = ConfigParser.ConfigParser()
        config.read(conf_file)
    
        # Get the polling intervals from the config file
        self._fast_poll_interval = config.getfloat('polling', 'fast_poll_interval')
        self._med_poll_multiple = config.getint('polling', 'med_poll_multiple')
        self._slow_poll_multiple = config.getint('polling', 'slow_poll_multiple')
        # fast_poll_interval is in seconds.  medium and slow are multiples of the
        # fast interval.  For example, values of 2.0, 15 & 60 will result in
        # polling every 2 seconds, 30 seconds and 2 minutes for fast, medium
        # and slow, respectively

        # Parameters for connecting to the SFA hardware
        self._sfa_user = config.get('ddn_hardware', 'sfa_user')
        self._sfa_password = config.get('ddn_hardware', 'sfa_password')
        
        # Parameters for connecting to the database
        self._db_user = config.get('database', 'db_user')
        self._db_password = config.get('database', 'db_password')
        self._db_host = config.get('database', 'db_host')
        self._db_name = config.get('database', 'db_name')


    def _time_series_init(self):
        '''
        Various initialization stats for all the time series data.  Must be called after the
        connection to the controller is established.
        '''
        
        # update the lun-to-vd mapping
        # This normally happens at the medium interval, but I need to do it here
        # so that I can store time series data by LUN instead of by virtual disk
        self._update_lun_map()

        # initialize the time series arrays
        vd_stats = SFAVirtualDiskStatistics.getAll()  # @UndefinedVariable
        self._time_series['lun_read_iops'] = { }
        self._time_series['lun_write_iops'] = { }
        self._time_series['lun_transfer_bytes'] = { }
        self._time_series['lun_read_bytes'] = { }
        self._time_series['lun_write_bytes'] = { }
        self._time_series['lun_forwarded_bytes'] = { }
        self._time_series['lun_forwarded_iops'] = { }
        for stats in vd_stats:
            index = stats.Index
            self._vd_stats[index] = stats

            # Note that these maps are indexed by Lun, not by virtual disk (despite
            # coming from SFAVirtualDiskStatistics objects)
            # 300 entries is 10 minutes of data at 2 second sample rate
            self._time_series['lun_read_iops'][self._vd_to_lun[index]] = SFATimeSeries( 300) 
            self._time_series['lun_write_iops'][self._vd_to_lun[index]] = SFATimeSeries( 300)
            self._time_series['lun_transfer_bytes'][self._vd_to_lun[index]] = SFATimeSeries( 300)
            self._time_series['lun_read_bytes'][self._vd_to_lun[index]] = SFATimeSeries( 300)
            self._time_series['lun_write_bytes'][self._vd_to_lun[index]] = SFATimeSeries( 300)
            self._time_series['lun_forwarded_bytes'][self._vd_to_lun[index]] = SFATimeSeries( 300)
            self._time_series['lun_forwarded_iops'][self._vd_to_lun[index]] = SFATimeSeries( 300)

# Don't need per-disk bandwidth & iops
#       disk_stats = SFADiskDriveStatistics.getAll()
#       self._time_series['dd_read_iops'] = { }
#       self._time_series['dd_write_iops'] = { }
#       self._time_series['dd_transfer_bytes'] = { }
#       for stats in disk_stats:
#           index = stats.Index
#           self._dd_stats[index] = stats
#           self._time_series['dd_read_iops'][index] = SFATimeSeries( 300)
#           self._time_series['dd_write_iops'][index] = SFATimeSeries( 300)
#           self._time_series['dd_transfer_bytes'][index] = SFATimeSeries( 300)

        

    def _check_labels(self):
        '''
        Verify the IO request size and latency labels are what we expect (and have
        hard coded into the database column headings)
        '''

        expected_size_labels = ['IO Size <=4KiB', 'IO Size <=8KiB', 'IO Size <=16KiB',
                'IO Size <=32KiB', 'IO Size <=64KiB', 'IO Size <=128KiB',
                'IO Size <=256KiB', 'IO Size <=512KiB', 'IO Size <=1MiB',
                'IO Size <=2MiB', 'IO Size <=4MiB', 'IO Size >4MiB']
        expected_lun_latency_labels = ['Latency Counts <=16ms', 'Latency Counts <=32ms',
                'Latency Counts <=64ms', 'Latency Counts <=128ms', 'Latency Counts <=256ms',
                'Latency Counts <=512ms','Latency Counts <=1s', 'Latency Counts <=2s',
                'Latency Counts <=4s', 'Latency Counts <=8s', 'Latency Counts <=16s',
                'Latency Counts >16s']
#        expected_dd_latency_labels = ['Latency Counts <=4ms', 'Latency Counts <=8ms',
#                'Latency Counts <=16ms', 'Latency Counts <=32ms', 'Latency Counts <=64ms',
#                'Latency Counts <=128ms', 'Latency Counts <=256ms', 'Latency Counts <=512ms',
#                'Latency Counts <=1s', 'Latency Counts <=2s', 'Latency Counts <=4s',
#                'Latency Counts >4s']

        vd_stats = SFAVirtualDiskStatistics.getAll()  # @UndefinedVariable
        for stats in vd_stats:
            if stats.IOSizeIndexLabels != expected_size_labels:
                raise UnexpectedClientDataException(
                        "Unexpected IO size index labels for %s virtual disk %d" % \
                                (self._get_host_name(), stats.Index))
            if stats.IOLatencyIndexLabels != expected_lun_latency_labels:
                raise UnexpectedClientDataException(
                        "Unexpected IO latency index labels for %s virtual disk %d" % \
                                (self._get_host_name(), stats.Index))
#        disk_stats = SFADiskDriveStatistics.getAll()  # @UndefinedVariable
#        # NOTE: getAll() is particularly slow for SFADiskDriveStatistics.  Might want to consider
#        # caching this value. (It's fetched up in _time_series_init())
#        for stats in disk_stats:
#            if stats.IOSizeIndexLabels != expected_size_labels:
#                raise UnexpectedClientDataException(
#                        "Unexpected IO size index labels for %s disk drive %d" % \
#                                (self._get_host_name(), stats.Index))
#            if stats.IOLatencyIndexLabels != expected_dd_latency_labels:
#                raise UnexpectedClientDataException(
#                        "Unexpected IO latency index labels for %s disk drive %d" % \
#                                (self._get_host_name(), stats.Index))

    
    def _get_host_name(self):
        '''
        Mostly a convenience function so we can map an object back to a
        human-readable name.
        ''' 
        return self._address


    def _get_time_series_average( self, series_name, device_num, span):
        '''
        Return the average value for the specified series and device
        calculated over the specified number of seconds.

        Returns a tuple: first value is the calculated average, second
        value is the actual timespan (in seconds) used to calculate
        the average.
        '''
        #TODO: need some protection against 'key not found' type of
        #errors for both the series name and device number
        return self._time_series[series_name][device_num].average(span)

                
    def _update_lun_map( self):
        presentations = SFAPresentation.getAll()  # @UndefinedVariable
        for p in presentations:
            self._vd_to_lun[p.VirtualDiskIndex] = p.LUN
        self.logger.debug( "Mapped %d virtual disks to LUNs"%len(self._vd_to_lun))
            
    
    def _verify_fw_version(self):
        '''
        Returns True if the controller firmware version is sufficiently new.
        Returns False and writes an error to the log if it's not.
        '''    
        fw_version = SFAController.getAll()[0].FWRelease  # @UndefinedVariable
        # DDN version strings are 4 numbers separated by periods
        
        fw_nums = fw_version.split('.')
        min_nums = MINIMUM_FW_VER.split('.')
        version_too_low = False
        for i in range(min(len(fw_nums), len(min_nums))):
            if int(fw_nums[i]) > int(min_nums[i]):
                break   # firmware is new enough
            if int(fw_nums[i]) == int(min_nums[i]):
                pass    # firmware *might* be new enough - keep looking
            elif int(fw_nums[i]) < int(min_nums[i]):
                # firmware definitely too old
                version_too_low = True
                break
        
        if version_too_low:
            self.logger.error("Controller version '%s' is too old.  Minimum version is '%s'"%(fw_version, MINIMUM_FW_VER))
        
        return not version_too_low
        

# NOTE: this function is commented out for now while I decide upon the best way to
# deal with exceptions thrown during the connection process.  At the moment, I'm
# just calling APIConnect() directly and passing any exceptions up the stack.     
#    def _sfa_connect(self):
#        '''
#        Log in to the DDN hardware
#        '''
#        try:
#            APIConnect( self._uri, (self._user, self._password))
#            # Note: this will throw an exception if it can't connect
#            # Known exceptions:
#            # ddn.sfa.core.APIContextException: -2: Invalid username and/or password
#            # pywbem.cim_operations.CIMError: (0, 'Socket error: [Errno -2] Name or service not known')
#            self._sfa_connected = True;
#        except APIContextException, e:
#            pass
#        #except CIMError, e:
#        #    pass

        
