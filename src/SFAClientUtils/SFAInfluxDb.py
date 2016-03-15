# Created on March 14, 2016
# 
# @author: Ross Miller
# 
# Copyright 2016 UT Battelle, LLC
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


import logging
from influxdb import InfluxDBClient


class SFAInfluxDb(object):
    '''
    Encapsulates the database related tasks into one class with a fairly simple interface.
    '''
    
    
    _expected_size_field_values = ['<=4KiB', '<=8KiB', '<=16KiB', '<=32KiB',
                                   '<=64KiB', '<=128KiB', '<=256KiB',
                                   '<=512KiB', '<=1MiB', '<=2MiB', '<=4MiB',
                                   '>4MiB']
    _expected_latency_field_values = ['<=16ms', '<=32ms', '<=64ms', '<=128ms',
                                      '<=256ms', '<=512ms','<=1s', '<=2s',
                                      '<=4s', '<=8s', '<=16s', '>16s']
    # Note: We're hard-coding the size and latency buckets rather than trying
    # to get them from the DDN API.  When SFAClient objects start up, they
    # verify that the size buckets that the DDN controllers are using match
    # what we expect.  If that ever changes, we'll obviously have to change
    # this code, too.



    def __init__(self, user, password, host, db_name):
        '''
        Connect to the InfluxDB server
        
        Note that we're deliberately *NOT* catching any exceptions that might
        be thrown.  There's really very little that this class could do to
        recover from any errors and without a database connection, this class
        is pretty useless.
        '''

        # Get the logger object
        self.logger = logging.getLogger( 'DDNTool_SFAInfluxDb')
        self.logger.debug( 'Creating instance of SFAInfluxDb')

        # open the database connection
        self._dbcon = InfluxDBClient(host=host, port=8086, username=user, password=password, database=db_name) 
        
    
    def update_lun_series( self, sfa_host_name, update_time, lun_num,
                           transfer_bytes, read_bytes, write_bytes,
                           forwarded_bytes, total_ios, read_ios, write_ios,
                           forwarded_ios, pool_state):
        '''
        Updates the various per-lun series in the databse
        '''
        
        # Schema:
        # Everything goes into one measurement (called lun_data)
        #   tags: sfa host name, lun number
        #   values: bytes read, bytes written, bytes transferred, bytes
        #   forwarded, read iops, write iops, forwarded iops and pool state
        
        if update_time == 0:
            # Sometimes, when the user has hit ctrl-C to exit the program,
            # we'll see several calls to this function with 0 for the
            # update time.  We'll ignore them.
            #
            # TODO: figure out *why* we get 0's and fix it!
            return
        
        # self.logger.debug( 'Updating lun data for lun %d at %s'%(lun_num, update_time))
        # This generages too much output, even for debug
        
        # this is what will be sent over to the influx server
        json_body = [ {
           "measurement": "lun_data",
           "tags": {
                "sfa_host": sfa_host_name,
                "lun_num": lun_num
            },
           "time": update_time * 1000000000,  # influx wants time in nano-seconds
           "fields": {
              "transfer_bytes":  transfer_bytes,
              "read_bytes":      read_bytes,
              "write_bytes":     write_bytes,
              "forwarded_bytes": forwarded_bytes,
              "total_iops":       total_ios,
              "read_iops":        read_ios,
              "write_iops":       write_ios,
              "forwarded_iops":   forwarded_ios,
              "pool_state":      pool_state,
            }
        } ]
        
        self._dbcon.write_points(json_body)
        
        
    def update_lun_request_size_series( self, sfa_host_name, update_time,
                                       lun_num, read_series, size_buckets):
        '''
        Update the read or write request size data (depending on the value
        of the read_series boolean) for one LUN on one client.
        size_buckets is a list containing the number of requests for each
        size and is expected to match the size values listed in
        _expected_size_field_values
        '''
        
        # Schema:
        # Measurement is named either 'read_request_sizes' or 
        # 'write_request_sizes' (depending on value of read_series)
        # Tags: sfa host name, lun number
        # Fields: one for each size bucket
        
        # sanity check
        if len(size_buckets) != len(self._expected_size_field_values):
            raise RuntimeError( "Invalid number of size buckets")
        
            
        # this is what will be sent over to the influx server
        json_body = [ {
            "time" : update_time * 1000000000,
            # influx wants time in nano-seconds
            "tags": {
                "sfa_host" : sfa_host_name,
                "lun_num" : lun_num 
            },
            "fields" :  {}
        } ]
        
        if (read_series):
            json_body[0]["measurement"] = "read_request_sizes"
        else:
            json_body[0]["measurement"] = "write_request_sizes"

        # add measurements to json_body for each bucket
        for i in range(len(size_buckets)):
            json_body[0]["fields"][self._expected_size_field_values[i]] = size_buckets[i]
            
        self._dbcon.write_points(json_body)   
                     
    
    def update_lun_request_latency_series( self, sfa_host_name, update_time,
                                       lun_num, read_series, latency_buckets):
        '''
        Update the read or write request latency data (depending on the value
        of the read_series boolean) for one LUN on one client.
        latency_buckets is a list containing the number of requests for each
        latency and is expected to match the values listed in 
        _expected_latency_field_values
        '''
        
        # Schema:
        # Measurement is named either 'read_request_latencies' or 
        # 'write_request_latencies' (depending on value of read_series)
        # Tags: sfa host name, lun number
        # Fields: one for each latency bucket
        
        # sanity check
        if len(latency_buckets) != len(self._expected_latency_field_values):
            raise RuntimeError( "Invalid number of size buckets")
        
            
        # this is what will be sent over to the influx server
        json_body = [ {
            "time" : update_time * 1000000000,
            # influx wants time in nano-seconds
            "tags": {
                "sfa_host" : sfa_host_name,
                "lun_num" : lun_num 
            },
            "fields" :  {}
        } ]
        
        if (read_series):
            json_body[0]["measurement"] = "read_request_latencies"
        else:
            json_body[0]["measurement"] = "write_request_latencies"

        # add measurements to json_body for each bucket
        for i in range(len(latency_buckets)):
            json_body[0]["fields"][self._expected_latency_field_values[i]] = latency_buckets[i]
            
        self._dbcon.write_points(json_body)   
