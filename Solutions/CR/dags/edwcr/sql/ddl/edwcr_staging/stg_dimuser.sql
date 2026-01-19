CREATE OR REPLACE TABLE {{ params.param_cr_stage_dataset_name }}.stg_dimuser (
dimsiteid INT64
, dimuserid INT64
, dimresourceid INT64
, dimresourceid_stakeholder INT64
, usercuid STRING
, userid STRING
, languageid STRING
, ctrappuserser INT64
, logid INT64
, runid INT64
, firstname STRING
, lastname STRING
, displayname STRING
, ctrinst_id STRING
, ctruserid STRING
, proftype INT64
, profdescription STRING
, dw_last_update_date_time DATETIME
)
  ;
