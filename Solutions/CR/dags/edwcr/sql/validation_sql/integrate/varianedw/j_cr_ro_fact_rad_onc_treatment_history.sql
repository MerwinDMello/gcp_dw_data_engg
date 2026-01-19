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

  SET srctablename = concat(srcdataset_id, '.', 'facttreatmenthist_stg');

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
                             ORDER BY CAST(dp.dimsiteid AS INT64),
                                      CAST(dp.facttreatmenthistoryid AS INT64)) AS fact_treatment_history_sk,
                            ra.patient_course_sk AS patient_course_sk,
                            pp.patient_plan_sk AS patient_plan_sk,
                            ra1.patient_sk AS patient_sk,
                            dp.dimlkpid_treatmentintenttype,
                            dp.dimlookupid_clinicalstatus,
                            dp.dimlookupid_planstatus,
                            dp.dimlookupid_fieldtechnique,
                            dp.dimlookupid_technique,
                            dp.dimlookupid_techniquelabel,
                            dp.dimlkpid_treatmentdeliverytyp,
                            rr.site_sk AS site_sk,
                            dp.facttreatmenthistoryid,
                            dp.completion_date_time,
                            dp.first_treatment_date_time,
                            dp.last_treatment_date_time,
                            dp.status_date_time,
                            dp.active_ind,
                            dp.planneddoserate,
                            dp.coursedosedelivered,
                            dp.coursedoseplanned,
                            dp.coursedoseremaining,
                            dp.othercoursedosedelivered,
                            dp.dosecorrection,
                            dp.totaldoselimit,
                            dp.dailydoselimit,
                            dp.sessiondoselimit,
                            dp.primary_ind,
                            dp.logid,
                            dp.runid,
                            'R' AS source_system_code,
                            datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
   FROM
     (SELECT facttreatmenthist_stg.dimsiteid,
             facttreatmenthist_stg.dimcourseid,
             facttreatmenthist_stg.dimplanid,
             facttreatmenthist_stg.dimpatientid,
             facttreatmenthist_stg.dimlkpid_treatmentintenttype,
             facttreatmenthist_stg.dimlookupid_clinicalstatus,
             facttreatmenthist_stg.dimlookupid_planstatus,
             facttreatmenthist_stg.dimlookupid_fieldtechnique,
             facttreatmenthist_stg.dimlookupid_technique,
             facttreatmenthist_stg.dimlookupid_techniquelabel,
             facttreatmenthist_stg.dimlkpid_treatmentdeliverytyp,
             facttreatmenthist_stg.facttreatmenthistoryid,
             CAST(trim(facttreatmenthist_stg.completeddatetime) AS DATETIME) AS completion_date_time,
             CAST(trim(facttreatmenthist_stg.firsttreatmentdate) AS DATETIME) AS first_treatment_date_time,
             CAST(trim(facttreatmenthist_stg.lasttreatmentdate) AS DATETIME) AS last_treatment_date_time,
             CAST(trim(facttreatmenthist_stg.statusdate) AS DATETIME) AS status_date_time,
             CASE facttreatmenthist_stg.isactive
                 WHEN 1 THEN 'Y'
                 WHEN 0 THEN 'N'
             END AS active_ind,
             facttreatmenthist_stg.planneddoserate,
             trim(facttreatmenthist_stg.coursedosedelivered) AS coursedosedelivered,
             trim(facttreatmenthist_stg.coursedoseplanned) AS coursedoseplanned,
             trim(facttreatmenthist_stg.coursedoseremaining) AS coursedoseremaining,
             trim(facttreatmenthist_stg.othercoursedosedelivered) AS othercoursedosedelivered,
             trim(facttreatmenthist_stg.dosecorrection) AS dosecorrection,
             trim(facttreatmenthist_stg.totaldoselimit) AS totaldoselimit,
             trim(facttreatmenthist_stg.dailydoselimit) AS dailydoselimit,
             trim(facttreatmenthist_stg.sessiondoselimit) AS sessiondoselimit,
             CASE facttreatmenthist_stg.primaryflag
                 WHEN 1 THEN 'Y'
                 WHEN 0 THEN 'N'
             END AS primary_ind,
             facttreatmenthist_stg.logid,
             facttreatmenthist_stg.runid
      FROM {{ params.param_cr_stage_dataset_name }}.facttreatmenthist_stg) AS dp
   INNER JOIN
     (SELECT ref_rad_onc_site.source_site_id,
             ref_rad_onc_site.site_sk
      FROM {{ params.param_cr_base_views_dataset_name }}.ref_rad_onc_site) AS rr ON rr.source_site_id = dp.dimsiteid
   LEFT OUTER JOIN
     (SELECT rad_onc_patient_course.patient_course_sk AS patient_course_sk,
             rad_onc_patient_course.source_patient_course_id,
             rad_onc_patient_course.site_sk
      FROM {{ params.param_cr_base_views_dataset_name }}.rad_onc_patient_course) AS ra ON dp.dimcourseid = ra.source_patient_course_id
   AND rr.site_sk = ra.site_sk
   LEFT OUTER JOIN
     (SELECT rad_onc_patient_plan.source_patient_plan_id,
             rad_onc_patient_plan.site_sk,
             rad_onc_patient_plan.patient_plan_sk
      FROM {{ params.param_cr_base_views_dataset_name }}.rad_onc_patient_plan) AS pp ON dp.dimplanid = pp.source_patient_plan_id
   AND rr.site_sk = pp.site_sk
   LEFT OUTER JOIN
     (SELECT rad_onc_patient.source_patient_id,
             rad_onc_patient.site_sk,
             rad_onc_patient.patient_sk
      FROM {{ params.param_cr_base_views_dataset_name }}.rad_onc_patient) AS ra1 ON dp.dimpatientid = ra1.source_patient_id
   AND rr.site_sk = ra1.site_sk) AS stg)
  );

  SET act_values_list =
  (
  SELECT SPLIT(SOURCE_STRING,',') values_list
  FROM (SELECT format('%20d', count(*)) AS source_string
FROM {{ params.param_cr_core_dataset_name }}.fact_rad_onc_treatment_history
WHERE fact_rad_onc_treatment_history.dw_last_update_date_time >= tableload_start_time - INTERVAL 1 MINUTE)
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
