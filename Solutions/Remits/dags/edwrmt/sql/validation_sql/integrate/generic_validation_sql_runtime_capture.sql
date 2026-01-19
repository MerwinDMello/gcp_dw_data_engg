##########################
## Variable Declaration ##
##########################

BEGIN
DECLARE
srctableid int64;
declare
sourcesysnm,srctablename,tgttablename,audit_type,tableload_run_time,audit_job_name,audit_status,tgtdataset_id string;
declare
tableload_start_time,tableload_end_time,audit_time,current_ts datetime;
SET
current_ts = datetime_trunc(current_datetime('US/Central'), SECOND);
SET 
srctableid = Null;
SET
sourcesysnm = @p_source;

SET
srctablename = Null;

SET
tgtdataset_id = 
(select arr[offset(1)]
from(
select split("{{ params.param_clm_core_dataset_name }}" , '.') as arr));

SET
tgttablename =concat(tgtdataset_id, '.', @p_targettable_name);

SET
audit_type ='NO_VALIDATION_SQL';
SET
tableload_start_time = @p_tableload_start_time;
SET
tableload_end_time = @p_tableload_end_time;
SET
tableload_run_time = CAST((tableload_end_time - tableload_start_time) AS STRING);
SET
audit_job_name = @p_job_name;
SET
audit_time = current_ts;

##Insert statement
INSERT INTO
 edwclm_ac.audit_control
VALUES
  (GENERATE_UUID(), cast(srctableid as int64), sourcesysnm, srctablename, tgttablename, audit_type, 0, 0, 
  cast(tableload_start_time as datetime), cast(tableload_end_time AS datetime),
  tableload_run_time,
   audit_job_name, audit_time, 'PASS' );
END; 