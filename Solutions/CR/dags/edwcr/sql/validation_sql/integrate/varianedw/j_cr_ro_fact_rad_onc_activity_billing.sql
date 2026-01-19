##########################
## Variable Declaration ##
##########################

BEGIN

  DECLARE srctableid, tolerance_percent, idx, idx_length INT64 DEFAULT 0;

  DECLARE expected_value, actual_value, difference NUMERIC;

  DECLARE sourcesysnm, srcdataset_id, srctablename, tgtdataset_id, tgttablename, audit_type, tableload_run_time, job_name, audit_status STRING;

  DECLARE tableload_start_time, tableload_end_time, audit_time, current_ts DATETIME;

  DECLARE exp_values_list, act_values_list ARRAY<STRING>;

  SET current_ts = DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND);

  SET srctableid = Null;

  SET sourcesysnm = 'varianedw';

  SET srcdataset_id = (select arr[offset(1)] from (select split('{{ params.param_cr_stage_dataset_name }}' , '.') as arr));

  SET srctablename = concat(srcdataset_id, '.', 'factactivitybilling_stg');

  SET tgtdataset_id = (select arr[offset(1)] from (select split('{{ params.param_cr_core_dataset_name }}' , '.') as arr));

  SET tgttablename =concat(tgtdataset_id, '.', @p_targettable_name);

  SET tableload_start_time = @p_tableload_start_time;

  SET tableload_end_time = @p_tableload_end_time;

  SET tableload_run_time = CAST((tableload_end_time - tableload_start_time) AS STRING);

  SET job_name = @p_job_name;

  SET audit_time = current_ts;

  SET tolerance_percent = 0;

  SET exp_values_list =
  (
  SELECT SPLIT(SOURCE_STRING,',') values_list
  FROM (SELECT format('%20d', count(*)) AS source_string
FROM
  (SELECT row_number() OVER (
                             ORDER BY dp.dimsiteid,
                                      dp.factactivitybillingid DESC) AS fact_activity_billing_sk
   FROM
     (SELECT factactivitybilling_stg.dimdoctorid,
             factactivitybilling_stg.dimdoctid_attdoncologist,
             factactivitybilling_stg.dimcourseid,
             factactivitybilling_stg.dimhospitaldepartmentid,
             factactivitybilling_stg.dimactivityid,
             factactivitybilling_stg.dimactivitytransactionid,
             factactivitybilling_stg.dimprocedurecodeid,
             factactivitybilling_stg.dimpatientid,
             factactivitybilling_stg.dimlkpid_activitycategory,
             factactivitybilling_stg.dimsiteid,
             factactivitybilling_stg.factactivitybillingid,
             factactivitybilling_stg.primaryglobalcharge,
             factactivitybilling_stg.primarytechnicalcharge,
             factactivitybilling_stg.primaryprofessionalcharge,
             factactivitybilling_stg.otherprofessionalcharge,
             factactivitybilling_stg.othertechnicalcharge,
             factactivitybilling_stg.otherglobalcharge,
             factactivitybilling_stg.chargeforecast,
             factactivitybilling_stg.actualcharge,
             factactivitybilling_stg.activitycost,
             trim(factactivitybilling_stg.accountbillingcode) AS accountbillingcode,
             factactivitybilling_stg.fromdateofservice,
             factactivitybilling_stg.todateofservice,
             factactivitybilling_stg.completeddatetime,
             factactivitybilling_stg.exporteddatetime,
             factactivitybilling_stg.markedcompleteddatetime,
             factactivitybilling_stg.creditexporteddatetime,
             factactivitybilling_stg.crediteddatetime,
             trim(factactivitybilling_stg.creditnote) AS creditnote,
             trim(factactivitybilling_stg.allmodifiercodes) AS allmodifiercodes,
             factactivitybilling_stg.creditamount,
             factactivitybilling_stg.isscheduled,
             factactivitybilling_stg.objectstatus,
             factactivitybilling_stg.logid,
             factactivitybilling_stg.runid
      FROM {{ params.param_cr_stage_dataset_name }}.factactivitybilling_stg) AS dp
   INNER JOIN
     (SELECT ref_rad_onc_site.source_site_id,
             ref_rad_onc_site.site_sk
      FROM {{ params.param_cr_core_dataset_name }}.ref_rad_onc_site
      FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central')) AS rr ON dp.dimsiteid = rr.source_site_id) AS stg)
  );

  SET act_values_list =
  (
  SELECT SPLIT(SOURCE_STRING,',') values_list
  FROM (SELECT format('%20d', count(*)) AS source_string
FROM {{ params.param_cr_core_dataset_name }}.fact_rad_onc_activity_billing
WHERE fact_rad_onc_activity_billing.dw_last_update_date_time >= tableload_start_time - INTERVAL 1 MINUTE)
  );

  SET idx_length = (SELECT ARRAY_LENGTH(act_values_list));

    LOOP
      SET idx = idx + 1;

      IF idx > idx_length THEN
        BREAK;
      END IF;

      SET expected_value = CAST(exp_values_list[ORDINAL(idx)] AS NUMERIC);
      SET actual_value = CAST(act_values_list[ORDINAL(idx)] AS NUMERIC);

      SET difference = 
        CASE 
        WHEN expected_value <> 0 Then CAST(((ABS(actual_value - expected_value)/expected_value) * 100) AS INT64)
        WHEN expected_value = 0 and actual_value = 0 Then 0
        ELSE actual_value
        END;

      SET audit_status = 
      CASE
        WHEN difference <= tolerance_percent THEN "PASS"
        ELSE "FAIL"
      END;

      IF idx = 1 THEN
        SET audit_type = "RECORD_COUNT";
      ELSE
        SET audit_type = CONCAT("VALIDATION_CNTRLID_",idx);
      END IF;

      --Insert statement
      INSERT INTO {{ params.param_cr_audit_dataset_name }}.audit_control
      VALUES
      (GENERATE_UUID(), cast(srctableid as int64), sourcesysnm, srctablename, tgttablename, audit_type,
      expected_value, actual_value, cast(tableload_start_time as datetime), cast(tableload_end_time AS datetime),
      tableload_run_time, job_name, audit_time, audit_status
      );

    END LOOP;

END;
