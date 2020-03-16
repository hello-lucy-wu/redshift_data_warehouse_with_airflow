 ## Redshift Data Warehouse With Airflow
 
This project is to use Apache Airflow to build data pipleline for a Redshift data warehouse. The [song data](https://s3.console.aws.amazon.com/s3/buckets/udacity-dend/song_data/?region=us-west-2&tab=overview) and [log data](https://s3.console.aws.amazon.com/s3/buckets/udacity-dend/log_data/?region=us-west-2&tab=overview) are in S3. <br /><br />
The script, etl.py, reads data from S3 and transforms them to create five different tables(Users, Artists, Songs, Time and Songplays). Each of the tables are written to parquet files in a separate analytics directory on S3.  <br /><br /> Songs table files are partitioned by year and artist. Time table files are partitioned by year and month. Songplays table files are partitioned by year and month.
The data structures of tables are the same as the one in my previous project  [data-modeling-with-postgres](https://github.com/hello-lucy-wu/data-modeling-with-postgres#Data). 

### Table of Contents
* [Tables](#Tables)
* [Steps to run scripts](#Steps)

### Tables
* There are four dimension tables and one fact tables.
    - Fact Table \
        songplays - records in log data associated with song plays i.e. records with page NextSong songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

    - Dimension Tables \
        users - users in the app
        user_id, first_name, last_name, gender, level

        songs - songs in music database
        song_id, title, artist_id, year, duration

        artists - artists in music database
        artist_id, name, location, latitude, longitude

        time - timestamps of records in songplays broken down into specific units
        start_time, hour, day, week, month, year, weekday


### Steps 
* create a running container, use volumes that map the directories on local machine where DAG definitions and plugins are hold, and the locations where Airflow reads them on the container `docker run -d -p 8080:8080 -v /Users/lucy/dev/data-science-nanodegree/airflow-project/dags:/usr/local/airflow/dags -v /Users/lucy/dev/data-science-nanodegree/airflow-project/plugins/:/usr/local/airflow/plugins puckel/docker-airflow webserver`
* go to `http://localhost:8080/admin/` to check the UI and set up a connection to your redshift cluster called `redshift`
* If you want to kill the container, execute `docker ps` in terminal to get the container ID, and then execute `docker stop <container id>` to kill it
 
