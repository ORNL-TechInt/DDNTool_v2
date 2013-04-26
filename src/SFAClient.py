'''
Created on Mar 22, 2013

@author: xmr
'''

import threading
import time

from SFATimeSeries import SFATimeSeries

#from ddn.sfa.api import *

    
    
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
        self._slow_poll_multple = 60 # multiples of _fast_poll_interval
        # values of 2.0, 15 & 60 will result in polling every 2 seconds,
        # 30 seconds and 2 minutes for fast, medium and slow, respectively
        
                
        # Lists of SFATimeSeries objects for specific values
        # (one series for each host channel on the controller)   
        self._host_channel_read_iops = []
        self._host_channel_write_iops = []
        self._host_channel_transfer_bytes = []
        
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
        
    
    def get_read_bw(self):
        '''
        getter
        '''
        #TODO: Implement me!
        pass
    
    def get_write_bw(self):
        '''
        getter
        '''
        #TODO: Implement me!
        pass
    
    def get_read_iops(self, host_channel, span):
        '''
        Return the  read IOPs for the specified host_channel averaged
        over the specified number of seconds
        
        Returns a tuple: first value is the calculated average, second
        is the actual timespan (in seconds) used to calculate the average 
        '''
        self._lock.acquire()
        #TODO: if host_channel is out of range, this will throw an exception
        # Probably need to catch, release the lock and then rethrow...
        average = self._host_channel_read_iops[host_channel].average(span)
        self._lock.release()
        return average
        
    
    def get_write_iops(self):
        '''
        getter
        '''
        #TODO: Implement me!
        pass
    
    def run(self):
        '''
        Main body of the background thread:  polls the SFA, post-processes the data and makes the results
        available to the getter functions.
        '''
        #TODO: Implement me!
        #print "Hello from thread ", self.getName()
        #print "Sleeping..."
        #sleep( 10)  # in seconds
        #print "Thread exiting"
        
        self._connect()

        host_channels_stats = SFAHostChannelStatistics.getAll()
              
              
        # initialize the time series arrays
        for i in range(len(host_channels_stats)):
            self._host_channel_read_iops.append(SFATimeSeries( 300)) # 10 minutes of data at 2 second sample rate
            self._host_channel_write_iops.append(SFATimeSeries( 300))
            self._host_channel_transfer_bytes.append(SFATimeSeries( 300))      
              
        next_fast_poll_time = 0
        fast_iteration = 0
        
        while not self._exit_requested:  # loop until we're told not to
            # Start with something trivial: host channel statistics
        
            while (time.time() < next_fast_poll_time):
                time.sleep(self._fast_poll_interval / 10) # wait for the poll time to come up
            
            next_fast_poll_time = time.time() + self._fast_poll_interval
            fast_iteration += 1
            
            ############# Fast Interval Stuff #######################
            for i in range(len(host_channels_stats)):
                self._host_channel_read_iops[i].append(host_channels_stats[i].ReadIOs[0])
                self._host_channel_write_iops[i].append(host_channels_stats[i].WriteIOs[0])
                self._host_channel_transfer_bytes[i].append(
                        host_channels_stats[i].KBytesTransfered[0] * 1024) # convert to bytes
            
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
        APIConnect( self._uri, (self._user, self._password))
        # Note: this will throw an exception if it can't connect
        # Known exceptions:
        # ddn.sfa.core.APIContextException: -2: Invalid username and/or password
        # pywbem.cim_operations.CIMError: (0, 'Socket error: [Errno -2] Name or service not known')
        self._connected = True;


