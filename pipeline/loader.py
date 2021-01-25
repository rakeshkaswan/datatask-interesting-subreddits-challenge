# -*- coding: utf-8 -*-
#!/usr/bin/python

"""
    This module is Loads data to database by preparing insert statement
"""
import psycopg2
from psycopg2 import extras

class Load(object):
    def populate(cursor, data, table):
        """
            populate prepares insert statement & execute query
        """        
        columns = ",".join(data.columns)
        values = "VALUES({})".format(",".join(["%s" for _ in data.columns]))
        query = "INSERT INTO {} ({}) {}".format(table, columns, values)
        extras.execute_batch(cursor, query, data.values)

        
    def populate_with_data(connection_params, data, table):
        """
            creating connection & populating data via populate
        """        
        try:
            with psycopg2.connect(**connection_params) as connection:
                cursor = connection.cursor()
                Load.populate(cursor, data, table)
                print("{} Table published!!".format(table.title()))
                connection.commit()
                cursor.close()

        except (Exception, psycopg2.Error) as error:
            print("Error while connecting to PostgreSQL", error)
            
    def execute(connection_params, query):
        """
            executing a table
        """      
        print ("Query - ", query)
        try:
            with psycopg2.connect(**connection_params) as connection:
                cursor = connection.cursor()
                cursor.execute(query)
                connection.commit()                
                cursor.close()

        except (Exception, psycopg2.Error) as error:
            print("Error while connecting to PostgreSQL", error)