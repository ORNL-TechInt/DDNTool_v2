'''
Created on May 3, 2013

@author: xmr
'''

import mysql.connector


# names for the various database tables
# Note: the names need to be unicode because that's what we get back from
# a SHOW TABLES statement
TABLE_NAMES = {
             "MAIN_TABLE_NAME" : u"Main",
             "DISK_TABLE_NAME" : u"Disk",
             "VIRTUAL_DISK_TABLE_NAME" : u"VirtDisk",
             "TIER_DELAY_TABLE_NAME" : u"TierDelays",
             "READ_REQUEST_SIZE_TABLE_NAME" : u"ReadRequestSizes",
             "READ_REQUEST_LATENCY_TABLE_NAME" : u"ReadRequestLatencies",
             "WRITE_REQUEST_SIZE_TABLE_NAME" : u"WriteRequestSizes",
             "WRITE_REQUEST_LATENCY_TABLE_NAME" : u"WriteRequestLatencies"

#define USER_TABLE_NAME             "Users"
 }
#

# Partially complete SQL statements for creating the request size
# latency tables
PARTIAL_LATENCY_TABLE_DEF = \
    "(Hostname VARCHAR(75) NOT NULL, LastUpdate TIMESTAMP, " \
    "Disk_Num SMALLINT UNSIGNED NOT NULL, " \
    "16ms INT UNSIGNED NOT NULL, " \
    "32ms  INT UNSIGNED NOT NULL, " \
    "64ms INT UNSIGNED NOT NULL, " \
    "128ms INT UNSIGNED NOT NULL, " \
    "256ms INT UNSIGNED NOT NULL, " \
    "512ms INT UNSIGNED NOT NULL, " \
    "1s INT UNSIGNED NOT NULL, " \
    "2s INT UNSIGNED NOT NULL, " \
    "4s INT UNSIGNED NOT NULL, " \
    "8s INT UNSIGNED NOT NULL, " \
    "16s INT UNSIGNED NOT NULL, " \
    "Longer_Than_16s INT UNSIGNED NOT NULL, " \
    "CONSTRAINT unique_disk UNIQUE (Hostname, Disk_Num), "  \
    "INDEX( Hostname), INDEX( Disk_Num) )" \
    "ENGINE=HEAP" \
    ";"

PARTIAL_SIZE_TABLE_DEF = \
    "(Hostname VARCHAR(75) NOT NULL, LastUpdate TIMESTAMP, " \
    "Disk_Num SMALLINT UNSIGNED NOT NULL, " \
    "4KiB INT UNSIGNED NOT NULL, " \
    "8KiB INT UNSIGNED NOT NULL, " \
    "16KiB INT UNSIGNED NOT NULL, " \
    "32KiB INT UNSIGNED NOT NULL, " \
    "64iB INT UNSIGNED NOT NULL, " \
    "128KiB INT UNSIGNED NOT NULL, " \
    "256KiB INT UNSIGNED NOT NULL, " \
    "512KiB INT UNSIGNED NOT NULL, " \
    "1MiB INT UNSIGNED NOT NULL, " \
    "2MiB INT UNSIGNED NOT NULL, " \
    "4MiB INT UNSIGNED NOT NULL, " \
    "Larger_Than_4MiB INT UNSIGNED NOT NULL, " \
    "CONSTRAINT unique_disk UNIQUE (Hostname, Disk_Num), "  \
    "INDEX( Hostname), INDEX( Disk_Num) )" \
    "ENGINE=HEAP" \
    ";"

# Note: We're hard-coding the size and latency buckets rather than trying to get
# them from the DDN API  (mainly because you can't have characters like <= in
# column names).  When SFAClient objects start up, they verify that the size
# buckets that the DDN controllers are using match what we expect.  If that
# ever changes, we'll obviously have to change this code, too.


class SFADatabase(object):
    '''
    Encapsulates the database related tasks into one class with a fairly simple interface.
    '''


    def __init__(self, user, password, host, db_name, init = False ):
        '''
        Connect to the database and create the tables (if necessary)
        
        Note that we're deliberately *NOT* catching any exceptions that might
        be thrown.  There's really very little that this class could do to recover
        from any errors and without a database connection and properly initialized
        tables, this class is pretty useless.
        '''

        self._dbcon = mysql.connector.connect(user = user, password = password,
                                              host = host, database = db_name)
        if init:            
            self._create_schema()
        
 
    def verify_main_table(self, sfa_clients):
        '''
        The main table is the only one we perform UPDATE queries on.  (For the others,
        we delete a row and insert a new one.)  The UPDATE query requires that the row
        already exists, so function ensures that we've got a row for every client
        in the list.
        
        Returns the number of rows that were actually inserted.  (Not terribly
        useful for production, but helpful for the unit tests.)
        '''
        
        
        find_query = "SELECT * FROM " + TABLE_NAMES['MAIN_TABLE_NAME'] + " WHERE Hostname = %s;"
        insert_query = "INSERT INTO " + TABLE_NAMES['MAIN_TABLE_NAME'] + " SET Hostname = %s;"
        
        num_inserts = 0
        cursor = self._dbcon.cursor()
        for client in sfa_clients:
            cursor.execute( find_query, (client,))
            if len(cursor.fetchall()) == 0:
                cursor.execute(insert_query, (client,))
                num_inserts += 1
            
        cursor.close()
        return num_inserts

    def update_main_table( self, sfa_client_name,
                           transfer_bw, 
                           read_iops, write_iops,
                           rebuild_bw, verify_bw):
        '''
        Updates one row in the main table.  The row for the particular client
        must already exist. (See verify_main_table())
        '''

        query = "UPDATE " + TABLE_NAMES['MAIN_TABLE_NAME'] + " SET " + \
            "Transfer_BW = %s, Read_IOPS = %s, Write_IOPS = %s, Rebuild_BW = %s, " + \
            "Verify_BW = %s WHERE Hostname = %s;"

        cursor = self._dbcon.cursor()
        cursor.execute( query, (str(transfer_bw), str(read_iops),
                                str(write_iops), str(rebuild_bw), str(verify_bw),
                                sfa_client_name))
        cursor.close()
                             

    def update_vd_table( self, sfa_client_name, vd_num, transfer_bw,
                         read_iops, write_iops, forwarded_bw,
                         forwarded_iops):
        '''
        Updates the row in the virtual disk table for the specified 
        client and virtual disk.
        '''

        replace_query = "REPLACE INTO " + TABLE_NAMES['VIRTUAL_DISK_TABLE_NAME'] + \
                "(Hostname, Disk_Num, Transfer_BW, Read_IOPS, Write_IOPS, " \
                "Forwarded_BW, Forwarded_IOPS) " \
                "VALUES( %s, %s, %s, %s, %s, %s, %s);"
        
        cursor = self._dbcon.cursor()
        cursor.execute( replace_query, (sfa_client_name, str(vd_num), str(transfer_bw),
                                        str(read_iops), str(write_iops),
                                        str(forwarded_bw), str(forwarded_iops)))
        cursor.close()

    def update_dd_table( self, sfa_client_name, vd_num, transfer_bw,
            read_iops, write_iops):
        '''
        Updates the row in the disk table for the specified 
        client and virtual disk.
        '''

        replace_query = "REPLACE INTO " + TABLE_NAMES['DISK_TABLE_NAME'] + \
                        "(Hostname, Disk_Num, Transfer_BW, Read_IOPS, Write_IOPS) " \
                        "VALUES( %s, %s, %s, %s, %s);"
     
        cursor = self._dbcon.cursor()
        cursor.execute( replace_query, (sfa_client_name, str(vd_num), str(transfer_bw),
                                        str(read_iops), str(write_iops)))
        cursor.close()

    def update_request_size_table( self, sfa_client_name, vd_num, read_table, size_buckets):
        '''
        Update the read or write request size data (depending on the value of the read_table
        boolean) for one virtual disk on one client.  size_buckets is a list containing the
        number of requests for each size and is expected to match the size values listed in
        the column headings.
        '''
        
        replace_query = "REPLACE INTO "
        if read_table:
            replace_query += TABLE_NAMES["READ_REQUEST_SIZE_TABLE_NAME"]
        else:    
            replace_query += TABLE_NAMES["WRITE_REQUEST_SIZE_TABLE_NAME"]

        replace_query += " VALUES( %s, CURRENT_TIMESTAMP(), %s" 
        
        for i in range(len(size_buckets)):
            replace_query += ", %s"
        replace_query += ");"
       
        values = (sfa_client_name, str(vd_num))
        for size in size_buckets:
                values += (str(size), )
        # Note: it seems like I shouldn't have to convert all the sizes to strings manually,
        # but I get strange mysql errors if I don't...

        cursor = self._dbcon.cursor()
        cursor.execute( replace_query, values)
        cursor.close()

    def update_request_latency_table( self, sfa_client_name, vd_num, read_table, latency_buckets):
        '''
        Update the read or write request size data (depending on the value of the read_table
        boolean) for one virtual disk on one client.  latency_buckets is a list containing
        the number of requests that were handled in each time frame and is expected to match
        the latency values listed in the column headings.
        '''

        replace_query = "REPLACE INTO "
        if read_table:
            replace_query += TABLE_NAMES["READ_REQUEST_LATENCY_TABLE_NAME"]
        else:
            replace_query += TABLE_NAMES["WRITE_REQUEST_LATENCY_TABLE_NAME"]

        replace_query += " VALUES( %s, CURRENT_TIMESTAMP(), %s"

        for i in range(len(latency_buckets)):
            replace_query += ", %s"
        replace_query += ");"

        values = (sfa_client_name, str(vd_num))
        for latency in latency_buckets:
                values += (str(latency), )
        # Note: it seems like I shouldn't have to convert all the values to strings manually,
        # but I get strange mysql errors if I don't...

        cursor = self._dbcon.cursor()
        cursor.execute( replace_query, values)
        cursor.close()

 
    def _create_schema(self):
        # Drop the old tables (since we're not storing long-term data, it's easier
        # to drop the old tables and re-create them than it is to use ALTER TABLE
        # statements.
        cursor = self._dbcon.cursor()
        cursor.execute( "SHOW TABLES;")
        results = cursor.fetchall()
        # results is a list of tuples - each tuple is one row.
        # In this case, there's only one value in each tuple: a table name
        cursor.close()

        values = TABLE_NAMES.values()         
        for result in results:
            if result[0] in values:
                cursor = self._dbcon.cursor()
                query = "DROP TABLE %s;"%result[0]
                cursor.execute( query)
                cursor.close()
        
        # create the new table(s)
        self._new_main_table()
        self._new_vd_table()
        self._new_dd_table()
        self._new_read_request_size_table()
        self._new_read_request_latency_table()
        self._new_write_request_size_table()
        self._new_write_request_latency_table() 
    
    def _new_main_table(self):
        '''
        Create the main db table (if it doesn't already exist)
        '''
        # self._dbcon must be valid
        
        table_def = \
        "CREATE TABLE " + TABLE_NAMES["MAIN_TABLE_NAME"] + \
        "(Hostname VARCHAR(75) KEY, LastUpdate TIMESTAMP, DDN_Name VARCHAR( 75)," \
        "DDN_Partner_Name VARCHAR( 75), Unit_Number TINYINT UNSIGNED, Alarm BOOL," \
        "Time_Since_Restart VARCHAR( 30), Total_Uptime VARCHAR( 30)," \
        "Transfer_BW FLOAT, Read_IOPS FLOAT, Write_IOPS FLOAT, Rebuild_BW FLOAT, Verify_BW FLOAT," \
        "Cache_Size INT UNSIGNED, Cache_Error BOOL, WC_Disabled BOOL," \
        "WC_Disable_Reason VARCHAR( 75), Disk_Failures SMALLINT UNSIGNED," \
        "INDEX( Hostname))" \
        "ENGINE=HEAP" \
        ";"

        cursor = self._dbcon.cursor()
        cursor.execute( table_def)
        cursor.close()

    def _new_vd_table(self):
        '''
        Create the db table that holds statistics on all the virtual disks
        '''

        table_def = \
        "CREATE TABLE " + TABLE_NAMES["VIRTUAL_DISK_TABLE_NAME"] + " "  \
        "(Hostname VARCHAR(75) NOT NULL, LastUpdate TIMESTAMP, " \
        "Disk_Num SMALLINT UNSIGNED NOT NULL, "  \
        "Transfer_BW FLOAT, READ_IOPS FLOAT, WRITE_IOPS FLOAT, "  \
        "Forwarded_BW FLOAT, FORWARDED_IOPS FLOAT, " \
        "CONSTRAINT unique_disk UNIQUE (Hostname, Disk_Num), "  \
        "INDEX( Hostname), INDEX( Disk_Num) )"  \
        "ENGINE=HEAP" \
        ";"

        cursor = self._dbcon.cursor()
        cursor.execute( table_def)
        cursor.close()

    def _new_dd_table(self):
        '''
        Create the db table that holds statistics on all the virtual disks
        '''
 
        # Note: this table is almost exactly the same as the virtual disk table.
        # Seems like we should be able to combine these 2 functions.
        table_def = \
        "CREATE TABLE " + TABLE_NAMES["DISK_TABLE_NAME"] + " "  \
        "(Hostname VARCHAR(75) NOT NULL, LastUpdate TIMESTAMP, " \
        "Disk_Num SMALLINT UNSIGNED NOT NULL, "  \
        "Transfer_BW FLOAT, READ_IOPS FLOAT, WRITE_IOPS FLOAT, "  \
        "CONSTRAINT unique_disk UNIQUE (Hostname, Disk_Num), "  \
        "INDEX( Hostname), INDEX( Disk_Num) )"  \
        "ENGINE=HEAP" \
        ";"

        cursor = self._dbcon.cursor()
        cursor.execute( table_def)
        cursor.close()


    def _new_read_request_size_table( self):
        '''
        Create the db table that holds read request size information.
        '''

        table_def = \
        "CREATE TABLE " + TABLE_NAMES["READ_REQUEST_SIZE_TABLE_NAME"] + \
        " " + PARTIAL_SIZE_TABLE_DEF

        cursor = self._dbcon.cursor()
        cursor.execute( table_def)
        cursor.close()

    def _new_write_request_size_table( self):
        '''
        Create the db table that holds write request size information.
        '''
        
        table_def = \
        "CREATE TABLE " + TABLE_NAMES["WRITE_REQUEST_SIZE_TABLE_NAME"] + \
        " " + PARTIAL_SIZE_TABLE_DEF

        cursor = self._dbcon.cursor()
        cursor.execute( table_def)
        cursor.close()

    def _new_read_request_latency_table( self):
        '''
        Create the db table that holds read request latency information.
        '''

        table_def = \
        "CREATE TABLE " + TABLE_NAMES["READ_REQUEST_LATENCY_TABLE_NAME"] + \
        " "  + PARTIAL_LATENCY_TABLE_DEF

        cursor = self._dbcon.cursor()
        cursor.execute( table_def)
        cursor.close()

    def _new_write_request_latency_table( self):
        '''
        Create the db table that holds write request latency information.
        '''

        table_def = \
        "CREATE TABLE " + TABLE_NAMES["WRITE_REQUEST_LATENCY_TABLE_NAME"] + \
        " "  + PARTIAL_LATENCY_TABLE_DEF

        cursor = self._dbcon.cursor()
        cursor.execute( table_def)
        cursor.close()

