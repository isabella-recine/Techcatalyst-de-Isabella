``` SQL

create TRANSIENT TABLE TECHCATALYST_DE.ir.SONGS_DIM (
         song_id VARCHAR(100),
         title VARCHAR(100),
         artist_id VARCHAR(100),
         year NUMBER(38,0),
         duration Float 
);

create TRANSIENT TABLE TECHCATALYST_DE.ir.USER_DIM (
        user_id NUMBER(38,0),
        first_name VARCHAR(100),
        last_name VARCHAR(100),
        gender VARCHAR(100),
        level VARCHAR(100)
        
);

create TRANSIENT TABLE TECHCATALYST_DE.ir.TIME_DIM (
        datetime TIMESTAMP_NTZ(9),
        start_time TIME,
        hour NUMBER(38,0),
        day NUMBER(38,0),
        week NUMBER(38,0),
        month NUMBER(38,0),
        year NUMBER(38,0),
        weekday VARCHAR(100)

);

create  TRANSIENT TABLE TECHCATALYST_DE.ir.ARTIST_DIM (
        artist_id VARCHAR(100),
        name VARCHAR(100),
        location VARCHAR(100),
        latitude FLOAT,
        longitude FLOAT

);

create TRANSIENT TABLE TECHCATALYST_DE.ir.SONGPLAYS_FACT (
        songplay_id NUMBER(38,0), 
        datetime_id NUMBER(38,0), 
        user_id NUMBER(38,0), 
        level VARCHAR(100), 
        song_id VARCHAR(100), 
        artist_id VARCHAR(100), 
        session_id VARCHAR(100), 
        location VARCHAR(100), 
        user_agent VARCHAR(100)
        
);

create stage techcatalyst_de.external_stage.ir_stage
    storage_integration = s3_int
    URL = 's3://techcatalyst-public/dw_stage/irecine/';

list @techcatalyst_de.external_stage.ir_stage;

create file format ir_parquet_format
type = 'PARQUET';

copy into techcatalyst_de.ir.songs_dim
from @techcatalyst_de.external_stage.ir_stage/songs_table/
pattern = '.*parquet*.'
file_format = 'ir_parquet_format'
on_error = continue
match_by_column_name = case_insensitive;

copy into techcatalyst_de.ir.artist_dim
from @techcatalyst_de.external_stage.ir_stage/artists_table/
pattern = '.*parquet*.'
file_format = 'ir_parquet_format'
on_error = continue
match_by_column_name = case_insensitive;

copy into techcatalyst_de.ir.songplays_fact
from @techcatalyst_de.external_stage.ir_stage/songplays_table/
pattern = '.*parquet*.'
file_format = 'ir_parquet_format'
on_error = continue
match_by_column_name = case_insensitive;

copy into techcatalyst_de.ir.time_dim
from @techcatalyst_de.external_stage.ir_stage/time_table/
pattern = '.*parquet*.'
file_format = 'ir_parquet_format'
on_error = continue
match_by_column_name = case_insensitive;

copy into techcatalyst_de.ir.user_dim
from @techcatalyst_de.external_stage.ir_stage/users_table/
pattern = '.*parquet*.'
file_format = 'ir_parquet_format'
on_error = continue
match_by_column_name = case_insensitive;



CREATE OR REPLACE TRANSIENT TABLE TEMP_SONGS_DIM (
    SONG_ID STRING,
    TITLE STRING,
    YEAR STRING,
    ARTIST_ID STRING,
    DURATION FLOAT,
    PARTITION_YEAR STRING,
    PARTITION_ARTIST_ID STRING
);


INSERT INTO TEMP_SONGS_DIM (SONG_ID, TITLE, YEAR, ARTIST_ID, DURATION, PARTITION_YEAR, PARTITION_ARTIST_ID)
SELECT
    $1:song_id::STRING AS SONG_ID,
    $1:title::STRING AS TITLE,
    $1:year::STRING AS YEAR,
    $1:artist_id::STRING AS ARTIST_ID,
    $1:duration::FLOAT AS DURATION,
    REGEXP_SUBSTR(METADATA$FILENAME, 'year=(\\d+)', 1, 1, 'e')::STRING AS PARTITION_YEAR,
    REGEXP_SUBSTR(METADATA$FILENAME, 'artist_id=([^/]+)', 1, 1, 'e')::STRING AS PARTITION_ARTIST_ID
FROM @techcatalyst_de.external_stage.ir_stage/songs_table/ (FILE_FORMAT => 'ir_parquet_format', PATTERN => '.*parquet.*');

```