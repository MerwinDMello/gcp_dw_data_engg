##########################
## Variable Declaration ##
##########################

BEGIN

DECLARE difference, control2_difference, control3_difference, src_rec_count, tgt_rec_count int64 DEFAULT 0;

DECLARE control2_src_rowcount, control2_tgt_rowcount, control3_src_rowcount, control3_tgt_rowcount NUMERIC DEFAULT 0;

DECLARE srctableid, tolerance_percent INT64;

DECLARE sourcesysnm, srcdataset_id, srctablename, tgtdataset_id, tgttablename, audit_type, tableload_run_time, job_name, audit_status STRING;

DECLARE tableload_start_time, tableload_end_time, audit_time, current_ts DATETIME;

SET current_ts = DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND);

SET srctableid = NULL;

SET sourcesysnm = ''; -- This needs to be added

SET srcdataset_id = (SELECT arr[offset(1)] FROM (SELECT SPLIT({{ params.param_pbs_stage_dataset_name }} , '.') AS arr));

SET srctablename = concat(srcdataset_id, '.','' ); -- This needs to be added

SET tgtdataset_id = (SELECT arr[offset(1)] FROM (SELECT SPLIT({{ params.param_pbs_core_dataset_name }} , '.') AS arr));

SET tgttablename =concat(tgtdataset_id, '.', @p_targettable_name);

SET audit_type ='RECORD_COUNT';

SET tableload_start_time = @p_tableload_start_time;

SET tableload_end_time = @p_tableload_end_time;

SET tableload_run_time = CAST((tableload_end_time - tableload_start_time) AS STRING);

SET job_name = @p_job_name;

SET audit_time = current_ts;

SET tolerance_percent = 3;

SET
(src_rec_count,control2_src_rowcount,control3_src_rowcount) = 
(

);

SET
(tgt_rec_count,control2_tgt_rowcount,control3_tgt_rowcount) =
(

);

SET
difference = CASE 
              WHEN src_rec_count <> 0 Then CAST(((ABS(tgt_rec_count - src_rec_count)/src_rec_count) * 100) AS INT64)
              WHEN src_rec_count =0 AND tgt_rec_count = 0 Then 0
              ELSE tgt_rec_count
              END;

SET
control2_difference = CASE 
              WHEN control2_src_rowcount <> 0 Then CAST(((ABS(control2_tgt_rowcount - control2_src_rowcount)/control2_src_rowcount) * 100) AS INT64)
              WHEN control2_src_rowcount =0 AND control2_tgt_rowcount = 0 Then 0
              ELSE CAST(control2_tgt_rowcount AS INT64)
              END;

SET
control3_difference = CASE 
              WHEN control3_src_rowcount <> 0 Then CAST(((ABS(control3_tgt_rowcount - control3_src_rowcount)/control3_src_rowcount) * 100) AS INT64)
              WHEN control3_src_rowcount =0 AND control3_tgt_rowcount = 0 Then 0
              ELSE CAST(control2_tgt_rowcount AS INT64)
              END;


SET
audit_status = CASE
    WHEN difference <= tolerance_percent AND control2_difference <= tolerance_percent AND control3_difference <= tolerance_percent THEN "PASS"
    ELSE "FAIL"
END;

##Insert statement
INSERT INTO
 edwpbs_ac.audit_control
VALUES
  (GENERATE_UUID(), cast(srctableid AS int64), sourcesysnm, srctablename, tgttablename, audit_type, 
  src_rec_count, tgt_rec_count, control2_src_rowcount, control2_tgt_rowcount, control3_src_rowcount, control3_tgt_rowcount,
  cast(tableload_start_time AS DATETIME), cast(tableload_end_time AS DATETIME),
  tableload_run_time, job_name, audit_time, audit_status );
END;