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

import copy
import logging
from influxdb import InfluxDBClient
from influxdb.exceptions import InfluxDBClientError

# Dictionary of the measurement names we'll use in the database
MEASUREMENT_NAMES = {
    "LUN_DATA" : "lun_data",
    "READ_REQUEST_SIZES" : "read_request_sizes",
    "WRITE_REQUEST_SIZES" : "write_request_sizes",
    "READ_REQUEST_LATENCIES" : "read_request_latencies",
    "WRITE_REQUEST_LATENCIES" : "write_request_latencies"
}


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



    def __init__(self, user, password, host, db_name, init = False):
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

        # This holds the JSON data that will be sent to the database
        self._json_body = []
        
        # open the database connection
        self._dbcon = InfluxDBClient(host=host, port=8086, username=user, password=password, database=db_name)
        
        if init:
            for name in MEASUREMENT_NAMES.values():
                try:
                    query = "DROP MEASUREMENT %s"%name
                    self._dbcon.query(query)
                except InfluxDBClientError, err:
                    # Ignore measurement not found; re-raise for all others
                    if err.message.find('measurement not found') == -1:
                        raise err
                            
    
    def flush_to_db(self):
        '''
        Send all the queued up data to the database server
        '''
        
        if self._json_body:
            self._dbcon.write_points(self._json_body)
            self._json_body = []


    def update_lun_series( self, sfa_host_name, update_time, lun_num,
                           transfer_bytes, read_bytes, write_bytes,
                           forwarded_bytes, total_ios, read_ios, write_ios,
                           forwarded_ios, pool_state):
        '''
        Updates the various per-lun series in the databse
        
        Note: This function only queues the values for later output.  To
        actually send anything to the database, you must call flush_to_db(). 
        '''
        
        # Schema:
        # Everything goes into one measurement (called lun_data)
        #   tags: sfa host name, lun number
        #   values: bytes read, bytes written, bytes transferred, bytes
        #   forwarded, read iops, write iops, forwarded iops and pool state    
        
        # self.logger.debug( 'Updating lun data for lun %d at %s'%(lun_num, update_time))
        # This generages too much output, even for debug
        
        # this is what will be sent over to the influx server
        self._json_body.append({
           "measurement": MEASUREMENT_NAMES["LUN_DATA"],
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
        })
        

    def update_lun_request_size_series( self, sfa_host_name, update_time,
                                       lun_num, read_series, size_buckets):
        '''
        Update the read or write request size data (depending on the value
        of the read_series boolean) for one LUN on one client.
        size_buckets is a list containing the number of requests for each
        size and is expected to match the size values listed in
        _expected_size_field_values
        
        Note: This function only queues the values for later output.  To
        actually send anything to the database, you must call flush_to_db(). 
        '''
        
        # Schema:
        # Measurement is named either 'read_request_sizes' or 
        # 'write_request_sizes' (depending on value of read_series)
        # Tags: sfa host name, lun number, bucket
        # Fields: value

        # sanity check
        if len(size_buckets) != len(self._expected_size_field_values):
            raise RuntimeError( "Invalid number of size buckets")
        
        # structure to hold one mesurement
        measurement = {
            "time" : update_time * 1000000000,
            # influx wants time in nano-seconds
            "tags": {
                "sfa_host" : sfa_host_name,
                "lun_num" : lun_num 
            },
            "fields" :  {}
        }
        
        if (read_series):
            measurement["measurement"] = MEASUREMENT_NAMES["READ_REQUEST_SIZES"]
        else:
            measurement["measurement"] = MEASUREMENT_NAMES["WRITE_REQUEST_SIZES"]

        # add measurements to json_body for each bucket
        for i in range(len(size_buckets)):
            measurement["tags"]["bucket"] = self._expected_size_field_values[i]
            measurement["fields"]["value"] = size_buckets[i]
            self._json_body.append( copy.deepcopy(measurement))
            


    def update_lun_request_latency_series( self, sfa_host_name, update_time,
                                       lun_num, read_series, latency_buckets):
        '''
        Update the read or write request latency data (depending on the value
        of the read_series boolean) for one LUN on one client.
        latency_buckets is a list containing the number of requests for each
        latency and is expected to match the values listed in 
        _expected_latency_field_values
        
        Note: This function only queues the values for later output.  To
        actually send anything to the database, you must call flush_to_db(). 
        '''
        
        # Schema:
        # Measurement is named either 'read_request_latencies' or 
        # 'write_request_latencies' (depending on value of read_series)
        # Tags: sfa host name, lun number, bucket
        # Fields: value
        
        # sanity check
        if len(latency_buckets) != len(self._expected_latency_field_values):
            raise RuntimeError( "Invalid number of size buckets")       
                   
        # structure to hold one mesurement
        measurement = {
            "time" : update_time * 1000000000,
            # influx wants time in nano-seconds
            "tags": {
                "sfa_host" : sfa_host_name,
                "lun_num" : lun_num 
            },
            "fields" :  {}
        }
        
        if (read_series):
            measurement["measurement"] = MEASUREMENT_NAMES["READ_REQUEST_LATENCIES"]
        else:
            measurement["measurement"] = MEASUREMENT_NAMES["WRITE_REQUEST_LATENCIES"]

        # add measurements to json_body for each bucket
        for i in range(len(latency_buckets)):
            measurement["tags"]["bucket"] = self._expected_latency_field_values[i]
            measurement["fields"]["value"] = latency_buckets[i]
            self._json_body.append( copy.deepcopy(measurement))
            
