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
             "TIER_DELAY_TABLE_NAME" : u"TierDelays"
#define USER_TABLE_NAME             "Users"
 }
#


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
                           read_bw, write_bw,
                           read_iops, write_iops,
                           rebuild_bw, verify_bw):
        '''
        Updates one row in the main table.  The row for the particular client
        must already exist. (See verify_main_table())
        '''

        query = "UPDATE " + TABLE_NAMES['MAIN_TABLE_NAME'] + " SET Read_BW = %s, " + \
            "Write_BW = %s, Read_IOPS = %s, Write_IOPS = %s, Rebuild_BW = %s, " + \
            "Verify_BW = %s WHERE Hostname = %s;"

        cursor = self._dbcon.cursor()
        cursor.execute( query, (str(read_bw), str(write_bw), str(read_iops),
                                str(write_iops), str(rebuild_bw), str(verify_bw),
                                sfa_client_name))
        cursor.close()
                             


 
    def _create_schema(self):
        # at the moment, we only have one table.  However, I expect that to change quickly
        
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
    
    
    def _new_main_table(self):
        '''
        Create the main db table (if it doesn't already exist)
        '''
        # self._dbcon must be valid
        
        table_def = \
        "CREATE TABLE " + TABLE_NAMES["MAIN_TABLE_NAME"] + \
        "(Hostname VARCHAR(75) KEY, LastUpdate TIMESTAMP, DDN_Name VARCHAR( 75)," + \
        "DDN_Partner_Name VARCHAR( 75), Unit_Number TINYINT UNSIGNED, Alarm BOOL," + \
        "Time_Since_Restart VARCHAR( 30), Total_Uptime VARCHAR( 30),Read_BW FLOAT," + \
        "Write_BW FLOAT, Read_IOPS FLOAT, Write_IOPS FLOAT, Rebuild_BW FLOAT, Verify_BW FLOAT," + \
        "Cache_Size INT UNSIGNED, Cache_Error BOOL, WC_Disabled BOOL," + \
        "WC_Disable_Reason VARCHAR( 75), Disk_Failures SMALLINT UNSIGNED," + \
        "INDEX( Hostname))" + \
        "ENGINE=HEAP" + \
        ";"

        cursor = self._dbcon.cursor()
        cursor.execute( table_def)

        
