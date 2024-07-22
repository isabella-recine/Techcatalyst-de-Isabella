# Million Song Data Warehouse Project

### Data Cleaning
To start the cleaning process I took the raw Json files from S3 and read them into databricks.

```sql
spark.conf.set("fs.s3a.access.key", "ACCESS KEY")
spark.conf.set("fs.s3a.secret.key","SECRET KEY")
```

Once the files where in databricks 

### SQL Querying
Here are the DDl scripes I used in Snowflake to further query the data and verify the results:

```sql
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
```


