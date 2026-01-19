CREATE OR REPLACE TABLE {{ params.param_cr_stage_dataset_name }}.stg_diminstitutelocation (
dimsiteid INT64
, diminstitutelocationid INT64
, dimhospitaldepartmentid INT64
, eventtype STRING
, locationdescription STRING
, buildingname STRING
, floorname STRING
, roomname STRING
, departmentname STRING
, locationgroupname STRING
, capacity INT64
, slotsize INT64
, locationopentime STRING
, locationclosetime STRING
, locationcomment STRING
, locationtel STRING
, primaryind STRING
, activeentryind STRING
, ctrloc_id INT64
, ctrbldg_id INT64
, ctrfloor_id INT64
, ctrroom_id INT64
, logid INT64
, runid INT64
, dw_last_update_date_time DATETIME
)
  ;
