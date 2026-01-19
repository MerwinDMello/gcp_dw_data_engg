CREATE OR REPLACE TABLE {{ params.param_cr_stage_dataset_name }}.stg_dimcourse (
dimsiteid INT64
, dimcourseid INT64
, dimpatientid INT64
, courseid STRING
, coursestartdatetime STRING
, notxsessionplanned INT64
, notxsessiondelivered INT64
, notxsessionremaining INT64
, dosedelivered STRING
, courseduration INT64
, comment_comment STRING
, ctrcourseser INT64
, dimlookupid_clinicalstatus INT64
, dimlookupid_treatmentintenttyp INT64
, dimdxsiteid INT64
, logid INT64
, runid INT64
, dw_last_update_date_time DATETIME
)
  ;
