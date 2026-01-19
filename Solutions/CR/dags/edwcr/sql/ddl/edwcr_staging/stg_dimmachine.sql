CREATE OR REPLACE TABLE {{ params.param_cr_stage_dataset_name }}.stg_dimmachine (
dimsiteid INT64
, dimmachineid INT64
, machinefullname STRING
, machinealiasname STRING
, machineid STRING
, schedulable STRING
, machinemodel STRING
, machinescale STRING
, machinetype STRING
, resourcetypenum INT64
, resourceobjectstatus STRING
, ctrresourceser INT64
, dimlookupid_radiationdevicetyp INT64
, maxdwelltimeperchannel STRING
, maxdwelltimeperpos STRING
, maxdwelltimepertreatment STRING
, timeresolution STRING
, sourcemovementtype STRING
, minstepsize STRING
, maxstepsize STRING
, numofdwellposperchannel INT64
, stepsizeresolution INT64
, postosourcedist STRING
, doseratemode STRING
, brachyexportpostprocessor STRING
, logid INT64
, runid INT64
, dw_last_update_date_time DATETIME
)
  ;
