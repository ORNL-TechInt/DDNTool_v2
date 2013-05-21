'''
Created on Mar 22, 2013

@author: xmr
'''

import threading
import time

from SFATimeSeries import SFATimeSeries

from ddn.sfa.api import *

    
    
class SFAClient( threading.Thread):
    '''
    A class that represents a client for the SFA API.  Specifically, each instance will connect to a DDN
    SFA controller and poll it repeatedly for certain data.  The instance will format and pre-process
    the data and make the results available via getter functions.
    '''


    def __init__(self, address, username, password):
        '''
        Constructor
        '''
        #TODO: Implement me!
        threading.Thread.__init__( self, name=address)
        
        self._lock = threading.Lock()
       
        self._address = address 
        self._uri = "https://" + address
        self._user = username
        self._password = password
        
        self._connected = False;
        self._exit_requested = False;
        
        # How often we poll the DDN hardware - some data needs to be polled
        # faster than others
        # TODO: we probably want to read these values from a config file
        self._fast_poll_interval = 2.0 # in seconds
        self._med_poll_multiple = 15 # multiples of _fast_poll_interval
        self._slow_poll_multiple = 60 # multiples of _fast_poll_interval
        # values of 2.0, 15 & 60 will result in polling every 2 seconds,
        # 30 seconds and 2 minutes for fast, medium and slow, respectively
        
                
        # Dictionaries of SFATimeSeries objects for specific values -
        # one series for each virtual disk on the controller.
        # The keys are actually integers derived from the ElementName
        # property.  Using a dictionary instead of a list because
        # numbers are apparently not guarenteed to be contiguous
        self._vd_read_iops = {}
        self._vd_write_iops = {}
        self._vd_transfer_bytes = {}
        self._vd_forwarded_bytes = {}
        self._vd_forwarded_iops = {}

        self._dd_read_iops = {}
        self._dd_write_iops = {}
        self._dd_transfer_bytes = {}
                                 
        
        self.start()    # kick off the background thread
        
    
    def is_connected(self):
        '''
        Returns True if the instance is connected to the DDN controller, logged in and able
        to communicate with it.
        '''
        return self._connected
    
    def stop_thread(self, wait = False, timeout = None):
        '''
        Requests thread to stop and optionally waits for it to do so.
        
        Returns True if the thread has stopped and false if it is still running.
        '''
        self._exit_requested = True;
        
        if wait:
            self.join(timeout)
        
        return ( not self.isAlive())
        
   
    def get_host_name(self):
        '''
        Mostly a convenience function so we can map an object back to a
        human-readable name.
        ''' 
        return self._address

    def get_vd_nums(self):
        '''
        Returns a sorted list of all the virtual disk numbers this client has
        '''
        self._lock.acquire()
        try:
            nums = sorted(self._vd_read_iops)
        finally:
            self._lock.release()
        
        return nums

    def get_dd_nums(self):
        '''
        Returns a sorted list of all the disk drive indexes on this client
        '''
        self._lock.acquire()
        try:
            nums = sorted(self._dd_read_iops)
        finally:
            self._lock.release()

        return nums


    def get_vd_transfer_bw(self, vd_num, span):
        ''' 
        Return the transfer bandwidth (in bytes/sec) for the specified
        virtual disk averaged over the specified number of seconds
                
        Returns a tuple: first value is the calculated average, second
        is the actual timespan (in seconds) used to calculate the average 
        '''
        #TODO: need some protection against list index out of range errors!
        self._lock.acquire()
        try:    
            average = self._vd_transfer_bytes[vd_num].average(span)
        finally:
            self._lock.release() # always release the lock, even if
                                 # an exception occurs above 
        return average
 
    def get_vd_read_iops(self, vd_num, span):
        '''
        Return the read IOPs for the specified virtual disk averaged
        over the specified number of seconds
        
        Returns a tuple: first value is the calculated average, second
        is the actual timespan (in seconds) used to calculate the average 
        '''
        #TODO: need some protection against list index out of range errors!
        self._lock.acquire()
        try:
            average = self._vd_read_iops[vd_num].average(span)
        finally:
            self._lock.release() # always release the lock, even if
                                 # an exception occurs above
        return average
        
    
    def get_vd_write_iops(self, vd_num, span):
        '''
        Return the write IOPs for the specified virtual disk averaged
        over the specified number of seconds
        
        Returns a tuple: first value is the calculated average, second
        is the actual timespan (in seconds) used to calculate the average 
        '''
        #TODO: need some protection against list index out of range errors!
        self._lock.acquire()
        try:
            average = self._vd_write_iops[vd_num].average(span)
        finally:
            self._lock.release() # always release the lock, even if
                                 # an exception occurs above
        return average

    def get_vd_forwarded_bw( self, vd_num, span):
        '''
        Returns the forwarded data bandwidth (in bytes/sec) for the specified
        virtual disk averaged over the specified number of seconds

        Returns a tuple: first value is the caluclated average, second
        is the actual timespan (in seconds) used to calculate the average
        '''
        #TODO: need some protection against list index out of range errors!
        self._lock.acquire()
        try:
            average = self._vd_forwarded_bytes[vd_num].average(span)
        finally:
            self._lock.release() # always release the lock, even if
                                 # an exception occurs above
        return average
        

    def get_vd_forwarded_iops( self, vd_num, span):
        '''
        Returns the forwarded IOPS for the specified virtual disk averaged
        over the specified number of seconds

        Returns a tuple: first value is the caluclated average, second
        is the actual timespan (in seconds) used to calculate the average
        '''
        #TODO: need some protection against list index out of range errors!
        self._lock.acquire()
        try:
            average = self._vd_forwarded_iops[vd_num].average(span)
        finally:
            self._lock.release() # always release the lock, even if
                                 # an exception occurs above
        return average
   

    def get_dd_transfer_bw(self, dd_num, span):
        ''' 
        Return the transfer bandwidth (in bytes/sec) for the specified
        disk drive averaged over the specified number of seconds
                
        Returns a tuple: first value is the calculated average, second
        is the actual timespan (in seconds) used to calculate the average 
        '''
        #TODO: need some protection against list index out of range errors!
        self._lock.acquire()
        try:
            average = self._dd_transfer_bytes[dd_num].average(span)
        finally:
            self._lock.release() # always release the lock, even if
                                 # an exception occurs above 
        return average


    def get_dd_read_iops(self, dd_num, span):
        '''
        Return the read IOPs for the specified disk drive averaged
        over the specified number of seconds
        
        Returns a tuple: first value is the calculated average, second
        is the actual timespan (in seconds) used to calculate the average 
        '''
        #TODO: need some protection against list index out of range errors!
        self._lock.acquire()
        try:
            average = self._dd_read_iops[dd_num].average(span)
        finally:
            self._lock.release() # always release the lock, even if
                                 # an exception occurs above
        return average


    def get_dd_write_iops( self, dd_num, span):
        '''
        Returns the write IOPS for the specified disk drive averaged
        over the specified number of seconds

        Returns a tuple: first value is the caluclated average, second
        is the actual timespan (in seconds) used to calculate the average
        '''
        #TODO: need some protection against list index out of range errors!
        self._lock.acquire()
        try:
            average = self._dd_write_iops[dd_num].average(span)
        finally:
            self._lock.release() # always release the lock, even if
                                 # an exception occurs above
        return average
                                                                                                                                                            

    def run(self):
        '''
        Main body of the background thread:  polls the SFA, post-processes the data and makes the results
        available to the getter functions.
        '''
       
        self._lock.acquire()
        self._connect()

        # initialize the time series arrays
        vd_stats = SFAVirtualDiskStatistics.getAll()
        for stats in vd_stats:
            index = stats.Index

            #300 entries is 10 minutes of data at 2 second sample rate
            self._vd_read_iops[index] = SFATimeSeries( 300) 
            self._vd_write_iops[index] = SFATimeSeries( 300)
            self._vd_transfer_bytes[index] = SFATimeSeries( 300)
            self._vd_forwarded_bytes[index] = SFATimeSeries( 300)
            self._vd_forwarded_iops[index] = SFATimeSeries( 300)

        disk_stats = SFADiskDriveStatistics.getAll()
        for stats in disk_stats:
            index = stats.Index
            self._dd_read_iops[index] = SFATimeSeries( 300)
            self._dd_write_iops[index] = SFATimeSeries( 300)
            self._dd_transfer_bytes[index] = SFATimeSeries( 300)

        self._lock.release()
  
        next_fast_poll_time = 0
        fast_iteration = 0
        
        while not self._exit_requested:  # loop until we're told not to
            # Start with something easy: virtual disk statistics
        
            while (time.time() < next_fast_poll_time):
                time.sleep(self._fast_poll_interval / 10) # wait for the poll time to come up
            
            next_fast_poll_time = time.time() + self._fast_poll_interval
            fast_iteration += 1
            
            ############# Fast Interval Stuff #######################

            ##Virtual Disk Statistics 
            vd_stats = SFAVirtualDiskStatistics.getAll()
            try:
                self._lock.acquire()  # need to lock the mutex before we modify the data series
                for stats in vd_stats:
                    index = stats.Index
                    
                    # Note: we actually get back 2 element lists - one element
                    # for each controller in the couplet.  In theory, one of those
                    # elements should always be 0.
                    self._vd_read_iops[index].append(stats.ReadIOs[0] + stats.ReadIOs[1])
                    self._vd_write_iops[index].append(stats.WriteIOs[0] + stats.WriteIOs[1])
                    self._vd_transfer_bytes[index].append(
                            (stats.KBytesTransferred[0] + stats.KBytesTransferred[1]) * 1024)
                    # Note: converted to bytes

                    self._vd_forwarded_bytes[index].append(
                            (stats.KBytesForwarded[0] + stats.KBytesForwarded[1]) * 1024)
                    # Note: converted to bytes 

                    self._vd_forwarded_iops[index].append(
                            stats.ForwardedIOs[0] + stats.ForwardedIOs[1])
            finally:
                self._lock.release()

            # Yes - deliberately unlocking & re-locking the mutex to give other
            # threads a chance to access the data

            ##Disk Statistics
            disk_stats = SFADiskDriveStatistics.getAll()
            try:
                self._lock.acquire()  # need to lock the mutex before we modify the data series
                for stats in disk_stats:
                    index = stats.Index

                    # Note: we actually get back 2 element lists - one element
                    # for each controller in the couplet.  In theory, one of those
                    # elements should always be 0.
                    self._dd_read_iops[index].append(stats.ReadIOs[0] + stats.ReadIOs[1])
                    self._dd_write_iops[index].append(stats.WriteIOs[0] + stats.WriteIOs[1])
                    self._dd_transfer_bytes[index].append(
                            (stats.KBytesTransferred[0] + stats.KBytesTransferred[1]) * 1024)
                    # Note: converted to bytes

            finally:
                self._lock.release()
            
            ############# Medium Interval Stuff #####################
            if (fast_iteration % self._med_poll_multiple == 0):
                # TODO: implement medium interval stuff
                pass
            
            ############# Slow Interval Stuff #######################
            if (fast_iteration % self._slow_poll_multiple == 0):
                # TODO: implement slow interval stuff
                pass
                
        
        # end of main while loop
    # end of run() 
            
    # other getters we're going to want:
    # - rebuild and verify bandwidth (not available in the API yet)
    # - "Tier Delay"  - no such command in the API.  Will have to compute it from other values
    # 
     
     
    def _connect(self):
        '''
        Log in to the DDN hardware
        '''
        try:
            APIConnect( self._uri, (self._user, self._password))
            # Note: this will throw an exception if it can't connect
            # Known exceptions:
            # ddn.sfa.core.APIContextException: -2: Invalid username and/or password
            # pywbem.cim_operations.CIMError: (0, 'Socket error: [Errno -2] Name or service not known')
            self._connected = True;
        except APIContextException, e:
            pass
        #except CIMError, e:
        #    pass
        


