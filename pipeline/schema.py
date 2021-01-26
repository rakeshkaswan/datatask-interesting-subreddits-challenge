# -*- coding: utf-8 -*-
#!/usr/bin/python

"""
    This module defines tables for Database 
"""
import psycopg2

class Registory(object):
    
    def all_posts_table():
        """
            Contains User Create Table Statement
        """        
        return f""" CREATE TABLE IF NOT EXISTS all_posts (
            created_utc TIMESTAMP,
            score INT,
            domain VARCHAR(128) ,
            id VARCHAR(16),
            title text,
            ups INT, 
            downs INT,
            num_comments INT, 
            permalink text, 
            selftext TEXT, 
            link_flair_text VARCHAR(128), 
            over_18 BOOL,       
            thumbnail VARCHAR(128), 
            subreddit_id VARCHAR(16), 
            edited BOOL, 
            link_flair_css_class VARCHAR(64),       
            author_flair_css_class VARCHAR(128), 
            is_self BOOL, 
            name VARCHAR(32), 
            url TEXT, 
            distinguished VARCHAR(32),
            load_date DATE
        )"""
        
    def posts_table(suffix):
        """
            Contains posts Table Statement
        """        
        return f""" CREATE TABLE IF NOT EXISTS posts_%s (
            created_utc TIMESTAMP,
            score INT,
            ups INT, 
            downs INT,
            permalink text, 
            id VARCHAR(16) UNIQUE,
            subreddit_id VARCHAR(16) 
        )"""%suffix
        
    def create_tables(connection_config, table_type, suffix = None):
        """
            Creates tables in the PostgresDB
        """
        try:
            with psycopg2.connect(**connection_config) as connection:
                cursor = connection.cursor()
                if suffix:
                    func = getattr(
                            Registory, 
                            "{}_table".format(table_type),
                            suffix
                        )(suffix)
                    name = "{}_{}".format(table_type, suffix)
                else:
                   func = getattr(
                           Registory, 
                           "{}_table".format(table_type),
                           suffix
                        )()
                   name = table_type
 
                print("Creating Table - {}".format(name))
                cursor.execute(func)
                print("Success!! {} Table Created".format(name))
                #cursor.execute(Registory.users_table())
                #print("Success!! User Table Created")
                connection.commit()
                cursor.close()
                
        except (Exception, psycopg2.Error) as error:
            print("Error while connecting to PostgreSQL", error)

    def prepare_db(config):
        """
            Creating tables
        """
        Registory.create_tables(config['DB'], "all_posts") 
        
        for year in config['transformation_year']:
            Registory.create_tables(config['DB'], "posts", year)
