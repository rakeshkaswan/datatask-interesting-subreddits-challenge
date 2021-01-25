# -*- coding: utf-8 -*-
#!/usr/bin/python

"""
    This module ingest data from csv to PostgreSQL table
"""
import numpy as np
import pandas as pd
from datetime import datetime as dt
from .loader import Load

class Inject(object):
    def __init__(self, config):
        """
            Initializing current date, config & column to data mapping 
        """
        self._date = str(dt.now().date())
        self._config = config
        self._column_types = {
            "created_utc" : "float64",
            "score" : "int16",
            "domain" : "string" ,
            "id" : "string",
            "title" : "string",
            "ups" : "int16", 
            "downs" : "int16",
            "num_comments" : "int8", 
            "permalink" : "string", 
            "selftext" : "object", 
            "link_flair_text" : "string", 
            "over_18": "bool",       
            "thumbnail" : "string", 
            "subreddit_id" : "string", 
            "edited": "object", 
            "link_flair_css_class" : "string",       
            "author_flair_css_class" : "string", 
            "is_self": "object", 
            "name" : "string", 
            "url" : "object", 
            "distinguished" : "string"
        }
        
    def _edit_mapping(self, value, created):
        """
            Cleaning edited field
            if empty return False
            if float compare with created_utc if > return True
        """
        comparison_value = str(value)
        if comparison_value == '':
            return False
        elif comparison_value.title() in ('True', 'False'):
            return eval(value.title())
        else:
            #If edited is float convert it to timestamp & check timestamp > created_utc
            edited_dt = pd.Timestamp(pd.to_numeric(value), unit='s')
            return edited_dt > created

    def load(self, file):
        """
            Reading & Loading csv into PostgreSQL table all_posts 
        """
        print ("Reading file : %s"%file)
        #Reading file in chunks
        self._posts_data = pd.read_csv(
                    file,
                    chunksize = 500000,
                    parse_dates=['created_utc'], 
                    date_parser = lambda date : pd.Timestamp(pd.to_numeric(date), unit='s'),
                    #na_values='',
                    dtype = self._column_types
                )
        
        print ("Running into chunks of  file 500000")
        i = 1
        #Looping over chunks
        for chunk_data in self._posts_data:
            print ("Running chunk {}".format(i))

            #Cleaning edited field
            chunk_data['edited'] = chunk_data.apply(
                    lambda df: self._edit_mapping(
                            df.edited, df.created_utc
                    ), 
                    axis = 1
                )
            
            #Filling NA except    'over_18', 'edited', 'is_self'                 
            chunk_data.fillna({col:'' for col in self._column_types if col not in ('over_18', 'edited', 'is_self')}, inplace = True)

            #Filling  'over_18', 'edited', 'is_self' NA with None
            for col in ('over_18', 'edited', 'is_self'):
                chunk_data[col] = np.where(chunk_data[col].isnull(), None, chunk_data[col])
                
            #abs ups, downs & load_date as current run date
            chunk_data['ups'] = chunk_data.ups.abs()
            chunk_data['downs'] = chunk_data.downs.abs()
            chunk_data['load_date'] = self._date

            #making unique list of year-month ex. 2008-12
            year_month = chunk_data.created_utc.dt.strftime("%Y-%m").unique()
            year_month = sorted(year_month.tolist())
            
            #loading data for year-month combination
            for item in year_month:                
                Load.populate_with_data(
                        self._config['DB'],
                        chunk_data[chunk_data.created_utc.dt.strftime("%Y-%m") == item],
                        "all_posts"
                    )
            print ("Loading chunk {} finished".format(i))
            i += 1
            
            