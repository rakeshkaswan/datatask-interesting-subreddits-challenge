# -*- coding: utf-8 -*-

"""
    This module querying data from PostgreSQL table
"""

import psycopg2
from tabulate import tabulate

class Query(object):
    def execute(connection_params, year):
        """
            creating connection & executing select statement for popular subreddit_id
        """      
        query = """ 
            SELECT 
                subreddit_id, max(score) score
            FROM 
                posts_{} 
            WHERE 
                ups > 5000 and downs > 5000 and 
                abs(ups - downs) < 10000
                
            group by 1
            order by 2 desc
            limit 10
        """.format(year)
        print ("Query - ", query)
        try:
            with psycopg2.connect(**connection_params) as connection:
                cursor = connection.cursor()
                cursor.execute(query)
                rows = cursor.fetchall()                
                print ("Result - ")
                data = [
                        ['subreddit_id', 'score'],                        
                    ]

                for row in rows:
                    data.append([row[0], row[1]])

                print(tabulate(data, headers="firstrow"))

                connection.commit()                
                cursor.close()

        except (Exception, psycopg2.Error) as error:
            print("Error while connecting to PostgreSQL", error)