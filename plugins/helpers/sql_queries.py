class SqlQueries:
    staging_events_table_create= ("""
        CREATE TABLE IF NOT EXISTS "staging_events" (
            artist              text,
            auth                text,
            firstName           text,
            gender              VARCHAR(1),
            itemInSession       int,
            lastName            text,
            length              numeric,
            level               text,
            location            text,
            method              VARCHAR(3),
            page                text,
            registration        numeric,
            sessionId           int,
            song                text,
            status              int,
            ts                  BIGINT,
            userAgent           text,
            userId              int
        )
    """)

    staging_songs_table_create = ("""
        CREATE TABLE IF NOT EXISTS "staging_songs" (
            num_songs           int,
            artist_id           text,
            artist_latitude     numeric,
            artist_longitude    numeric,
            artist_location     text,
            artist_name         text,
            song_id             text,
            title               text,
            duration            numeric,
            year                int
        )
        diststyle all;
    """)

    songplays_table_create = ("""
        CREATE TABLE IF NOT EXISTS "songplays" (
            songplay_id         INT IDENTITY(0,1) PRIMARY KEY,
            start_time          TIMESTAMP,
            user_id             INTEGER NOT NULL,
            level               text NOT NULL,
            song_id             text NOT NULL,
            artist_id           text NOT NULL,
            session_id          INTEGER NOT NULL,
            location            text,
            user_agent          text
        );
    """)

    users_table_create = ("""
        CREATE TABLE IF NOT EXISTS "users" (
            user_id             INTEGER PRIMARY KEY,
            first_name          text,
            last_name           text,
            gender              VARCHAR(1),
            level               text NOT NULL
        )
        diststyle all;
    """)

    songs_table_create = ("""
        CREATE TABLE IF NOT EXISTS "songs" (
            song_id             text PRIMARY KEY,
            title               text,
            artist_id           text NOT NULL,
            year                INTEGER,
            duration            numeric
        )
        diststyle all;
    """)

    artists_table_create = ("""
        CREATE TABLE IF NOT EXISTS "artists" (
            artist_id           text PRIMARY KEY,
            name                text,
            location            text,
            latitude            numeric,
            longitude           numeric
        )
        diststyle all;
    """)

    time_table_create = ("""
        CREATE TABLE IF NOT EXISTS "time" (
            start_time          TIMESTAMP PRIMARY KEY sortkey,
            hour                INTEGER NOT NULL,
            day                 INTEGER NOT NULL,
            week                INTEGER NOT NULL,
            month               INTEGER NOT NULL,
            year                INTEGER NOT NULL,
            weekday             INTEGER NOT NULL
        )
        diststyle all;
    """)    

    songplays_table_insert = ("""
        insert into songplays (
            start_time,          
            user_id,             
            level,               
            song_id,             
            artist_id,           
            session_id,          
            location,
            user_agent
        )
        (
            SELECT
                events.start_time, 
                events.userid, 
                events.level, 
                songs.song_id, 
                songs.artist_id, 
                events.sessionid, 
                events.location, 
                events.useragent
                FROM (SELECT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time, *
            FROM staging_events
            WHERE page='NextSong') events
            LEFT JOIN staging_songs songs
            ON events.song = songs.title
                AND events.artist = songs.artist_name
                AND events.length = songs.duration
            where events.userid is not null and events.level is not null and songs.song_id is not null and songs.artist_id is not null and events.sessionid is not null 
        );
    """)

    users_table_insert = ("""
        SELECT distinct userid, firstname, lastname, gender, level
        FROM staging_events
        WHERE page='NextSong'
    """)

    songs_table_insert = ("""
        SELECT distinct song_id, title, artist_id, year, duration
        FROM staging_songs
    """)

    artists_table_insert = ("""
        SELECT distinct artist_id, artist_name, artist_location, artist_latitude, artist_longitude
        FROM staging_songs
    """)

    time_table_insert = ("""
        SELECT start_time, extract(hour from start_time), extract(day from start_time), extract(week from start_time), 
               extract(month from start_time), extract(year from start_time), extract(dayofweek from start_time)
        FROM songplays
    """)