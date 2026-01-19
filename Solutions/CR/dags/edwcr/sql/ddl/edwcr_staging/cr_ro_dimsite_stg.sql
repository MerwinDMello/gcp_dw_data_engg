CREATE OR REPLACE TABLE {{ params.param_cr_stage_dataset_name }}.cr_ro_dimsite_stg (
dimsiteid INT64 NOT NULL
, siteguid STRING
, sitecode STRING
, sitename STRING
, servername STRING
, sitedescription STRING
, serveripaddress STRING
, auraversion STRING
, auralastinstalleddate STRING
, registrationdate STRING
, hstryusername STRING
, hstrydatetime STRING
, dw_last_update_date_time DATETIME NOT NULL
)
  ;
