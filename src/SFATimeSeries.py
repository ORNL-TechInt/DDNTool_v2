'''
Created on 12 April, 2013

@author: xmr
'''

import time

class EmptyTimeSeriesException(Exception):
    '''
    An exception for indicating the time series we're trying
    to work with is empty.
    '''
    pass # Don't need anything other than what's in the base class


class SFATimeSeries( object):
    '''
    Used to hold time series data (specifically from the DDN SFA controllers)
    
    Most of the SFA API queries return the number of things since the controller
    was rebooted.  As such, they are expected to be continuously increasing.  Thus,
    to compute for example the average read iops over the last 10 minutes, we only
    need to subtract the most recent value from the one 10 minutes ago (and then
    divide by 600 to get the I/O's per second).  We specifically *don't* need to 
    look at all the values in between.
    
    In truth, if we knew in advance what time spans we'll want to calculate averages
    for, we wouldn't have to keep all the values in between...
    '''
    
    def __init__(self, max_size = None):
        self._series = []
        self._max_size = max_size
    
    def size(self):
        '''
        Returns the number of items in the series
        '''
        return len(self._series)
    
    def get(self, item_num):
        '''
        Returns a tuple of (value, timestamp) for the requested item
        
        Throws an IndexError if item_num is out of range
        '''
        return self._series[item_num]
    
    def flush(self):
        '''
        Delete all the values from the series
        '''
        self._series = []
    
    def average(self, span):
        '''
        Computes the average value over the last 'span' seconds
        
        Computes average "things per second" (where things are I/O Ops, bytes written,
        etc...) over the requested span.  Returns a tuple of the computed value and
        the actual span of seconds that it covered.  (For example, if values are added
        every 2 seconds, but a 5 second average is requested, the actual span will be
        4 seconds.)
        '''
       
        # Sanity check - we need at least to values to compute a meaningful average
        if len(self._series) < 2:
            raise EmptyTimeSeriesException()

        # Normal case: find the value who's time stamp is closest to what we want
        # and compute the average using it and the most recent value
        last_index = len(self._series) - 1
        first_index = self._binary_search( self._series[last_index][1] - span)
        
        # Sanity check:  If we were called with a very small span value, the binary search
        # function could return last_index as the best choice.  If first_index == last_index
        # though, we'd get a divide-by-zero error.
        if first_index == last_index:
            first_index = last_index - 1
              
        average = ((self._series[last_index][0] - self._series[first_index][0]) /
                   (self._series[last_index][1] - self._series[first_index][1]))
        average = abs( average)
        return (average, self._series[last_index][1] - self._series[first_index][1]) 
    
    def append(self, value):
        '''
        Adds one value to the time series and - if the max size has been
        exceeded - drops the oldest value.
        '''
        
        self._series.append((value, time.time()))
        
        # Remove any values that are too old
        if ( self._max_size and (len(self._series) > self._max_size)):
            self._series = self._series[len(self._series) - self._max_size:]
                            
        
    def _binary_search(self, timeval):
        '''
        Search the data series and return the (value, time) tuple that is
        closest to the requested time.
        '''
        # Can't use the standard bisect module because we have a series
        # of tuples, not individual values...
        min_index = 0;
        max_index = len(self._series) - 1
        # loop until we've narrowed it down to the two indexes that bracket our desired time value
        while (max_index - min_index) > 1:
            m = (min_index + max_index) / 2
            if self._series[m][1] < timeval:
                min_index = m
            elif self._series[m][1] > timeval:
                max_index = m
            else:  
                # we found an exact match for the time value
                # this will almost never happen...
                return m
        
        # now pick the index with the closer time value
        if (timeval - self._series[min_index][1]) < (self._series[max_index][1] - timeval):
            return min_index
        else:
            return max_index
        
    
