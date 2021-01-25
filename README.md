# Overview
The solution is created using Docker containerization. Latest CSV file is download from `S3 HTTPS` each time when we run code. Python (Pandas) library is utilised for reading & exploring (checking data columns & types) this file. Below things are considered for data exploration - 

* Checking data type for columns - float, bool, string, number
* Checking unique, min, max values & max length of string columns (max length is char count of string in the column)
* Relevant data mapping is created to read data efficiently using pandas.read_csv with below Schema
    ```
    | Columns                | Data Types   |
    |------------------------|--------------|
    | created_utc            | float64      |
    | score                  | int16        |
    | domain                 | string       |
    | id                     | string       |
    | title                  | string       |
    | ups                    | int16        |
    | downs                  | int16        |
    | num_comments           | int8         |
    | permalink              | string       |
    | selftext               | object       |
    | link_flair_text        | string       |
    | over_18                | string       |
    | thumbnail              | string       |
    | subreddit_id           | string       |
    | edited                 | object       |
    | link_flair_css_class   | string       |
    | author_flair_css_class | string       |
    | is_self                | object       |
    | name                   | string       |
    | url                    | object       |
    | distinguished          | string       |
    ```


# Ingestion
For data ingestion all above things are considered & followed Below steps - 
* `all_posts` table created with below schema in Postgres DB
    ```
    | Columns                | Data Types   |
    |------------------------|--------------|
    | created_utc            | TIMESTAMP    |
    | score                  | INT          |
    | domain                 | VARCHAR(128) |
    | id                     | VARCHAR(16)  |
    | title                  | text         |
    | ups                    | INT          |
    | downs                  | INT          |
    | num_comments           | INT          |
    | permalink              | text         |
    | selftext               | TEXT         |
    | link_flair_text        | VARCHAR(128) |
    | over_18                | BOOL         |
    | thumbnail              | VARCHAR(128) |
    | subreddit_id           | VARCHAR(16)  |
    | edited                 | BOOL         |
    | link_flair_css_class   | VARCHAR(64)  |
    | author_flair_css_class | VARCHAR(128) |
    | is_self                | BOOL         |
    | name                   | VARCHAR(32)  |
    | url                    | TEXT         |
    | distinguished          | VARCHAR(32)  |
    | load_date              | DATE         |
    ```

* Reading data in chunks using pandas.read_csv to avoid memory related failures
* created_utc is mapped from float to timestamp
* edited column is cleaned 
    * if value is float, convert it to timestamp & compare with created_utc, if the time is greater means this is edited
    * If value is empty it means not edited fill with False
* ups, downs columns are having -ve values, these are converted to +ve using absolute. As ups & down are voting/likes/dislikes, these can't be -ve
* NaN is filled with empty in case of string while None in case of bool
* Loaded this in `all_posts` table with `load_date` as the current date
    * `load_date` will help in transformation for new & existing records during daily runs



# Transformation
After data ingestion, transformation is divided into 2 parts by considering daily runs. Once ingestion is complete transformation utilizes `load_date`, so it can consider only latest records.

* Part 1 - Update query is created for existing records
    ```
    UPDATE 
        posts_2013 AS yp
    SET 
        score = ap.score, 
        ups = ap.ups, 
        downs = ap.downs
    FROM 
        all_posts AS ap
    WHERE 
        yp.id = ap.id and 
        ap.load_date = '2021-01-24'    
    ```
* Part 2 - insert query is created for new records
    ```
    INSERT INTO
        posts_2013 (created_utc, score, ups, downs, permalink, id, subreddit_id)
    SELECT
        ap.created_utc, ap.score, ap.ups, ap.downs, ap.permalink, ap.id, ap.subreddit_id
    FROM
        all_posts ap
    LEFT JOIN
        posts_2013 yp
        ON
            ap.id = yp.id
    WHERE
        extract( year from ap.created_utc ) = 2013 and
        ap.load_date = '2021-01-24' and
        yp.id is null
    ```

* Schema for Transformation Table - posts_2013
    ```
    | Columns      | Data Types          |
    |--------------|---------------------|
    | created_utc  | TIMESTAMP           |
    | score        | INT                 |
    | ups          | INT                 |
    | downs        | INT                 |
    | permalink    | text                |
    | id           | VARCHAR(16) UNIQUE  |
    | subreddit_id | VARCHAR(16)         |
    ```
# Querying
Below query selects top 10 `most intersting subreddits` - 
* Query
    ```
     SELECT 
            subreddit_id, max(score) score
        FROM 
            posts_2013 
        WHERE 
            abs(ups) > 5000 and abs(downs) > 5000 and 
            abs(abs(ups)-abs(downs)) < 10000
            
    group by 1
    order by 2 desc
    limit 10;
    ``` 
* Result
    ```
    | subreddit_id | max  |
    |--------------|------|
    | t5_2qh0u     | 9585 |
    | t5_2r8tu     | 9022 |
    | t5_2qh13     | 8797 |
    | t5_2qzb6     | 8447 |
    | t5_2qqjc     | 8198 |
    | t5_2qh03     | 8195 |
    | t5_2qh3v     | 7881 |
    | t5_2s7tt     | 7813 |
    | t5_2qh1e     | 7689 |
    | t5_2ti4h     | 7353 |
    ```

# How to run code
* First clone this repository on local using
    ```
    git clone git@github.com:rakeshkaswan/datatask-interesting-subreddits-challenge.git
    ```
* Make sure that `docker` client is installed to run this task, after clone, using terminal move to this directory `datatask-interesting-subreddits-challenge` and run below commands sequentially - 
    * `docker-compse bulid`
    * `docker-compose up`
    
Wait sometimes after `docker-compose up`, it will prepare database & execute data Ingestion, Transformation & Querying. Once it displays `datatask-interesting-subreddits-challenge_etl_1 exited with code 0`, means data is ready for querying and follow below steps - 
* Access `docker bash` in new terminal window using
    ```
    docker exec -it datatask-interesting-subreddits-challenge_postgres_1 bash 
    ```
* Access DB using
    ```
    root@310d1dfbfc1b:/# psql -U user -W database
    (This will prompt for password, type password as password)
    ```
* List tables using `\d`
    ```
     database=# \d
                  List of relations
     Schema |        Name         | Type  | Owner 
    --------+---------------------+-------+-------
     public | all_posts           | table | user
     public | posts_2013          | table | user
    ```
* Now query any table
    ```
    database=# select distinct load_date from all_posts;
     load_date  
    ------------
     2021-01-24
    (1 row)
    ```
* If want to execute pipeline again (incase source data is updated/for daily run), use below command - 
    ```
    docker-compose run etl
    (This will start executing pipeline again & populate db with new data, before running this command make sure postgres container should be up)
    ```


It's all set, happily play with data!!! 
