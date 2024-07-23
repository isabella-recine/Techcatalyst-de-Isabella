CREATE OR REPLACE STAGE TECHCATALYST_DE.EXTERNAL_STAGE.ir_AWS_STAGE
        STORAGE_INTEGRATION = s3_int
        URL='s3://techcatalyst-public/ir/';
 
 
CREATE OR REPLACE FILE FORMAT csv_format
TYPE = 'CSV'
FIELD_OPTIONALLY_ENCLOSED_BY = '"'
SKIP_HEADER = 1;
 
 
create or replace transient table techcatalyst_de.ir.test
(name string,
favnumber number
);
 
 
create or replace pipe techcatalyst_de.external_stage.isabella_pipe
auto_ingest = True
as 
copy into techcatalyst_de.ir.test
from  @TECHCATALYST_DE.EXTERNAL_STAGE.ir_AWS_STAGE
FILE_FORMAT = 'csv_format';

list @TECHCATALYST_DE.EXTERNAL_STAGE.ir_AWS_STAGE;
 
show pipes;

 
select * from TECHCATALYST_DE.ir.TEST;
