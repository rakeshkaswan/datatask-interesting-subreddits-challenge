# -*- coding: utf-8 -*-
#!/usr/bin/python

"""
    This module is utilizes data & transform as per need
"""

from .loader import Load

 
class Transform(object):
    def __init__(self, config, execution_date):
        """
            Data Transform init function which takes config , execution_date to prepare posts_year table
        """        
        self._execution_date = execution_date

        #creating posts table for year
        for year in config['transformation_year']:
            self._year = year            
            Load.execute(
                    config['DB'],
                    self._update_existing()
            )
            Load.execute(
                    config['DB'],
                    self._insert_new()
            )
        
        
    
    def _update_existing(self):
        """
            Update existing data in posts_{year} from all_posts statement
        """        
        return """
                UPDATE 
                    posts_{year} AS yp
                SET 
                    score = ap.score, 
                    ups = ap.ups, 
                    downs = ap.downs
                FROM 
                    all_posts AS ap
                WHERE 
                    yp.id = ap.id and 
                    ap.load_date = '{date}'
            """.format(year = self._year, date = self._execution_date)
        
    
    def _insert_new(self):
        """
            insert new data in posts_{year} from all_posts statement
        """        
        return """
                INSERT INTO
                    posts_{year} (created_utc, score, ups, downs, permalink, id, subreddit_id)
                SELECT
                    ap.created_utc, ap.score, ap.ups, ap.downs, ap.permalink, ap.id, ap.subreddit_id
                FROM
                    all_posts ap
                LEFT JOIN
                    posts_{year} yp
                    ON
                        ap.id = yp.id
                WHERE
                    extract( year from ap.created_utc ) = {year} and
                    ap.load_date = '{date}' and
                    yp.id is null
            """.format(year = self._year, date = self._execution_date)