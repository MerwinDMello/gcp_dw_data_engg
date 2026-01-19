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

  SET sourcesysnm = 'metriq';

  SET srcdataset_id = (select arr[offset(1)] from (select split('{{ params.param_cr_stage_dataset_name }}' , '.') as arr));

  SET srctablename = concat(srcdataset_id, '.', '');

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
  FROM (WITH sc AS
  (SELECT emr_date.*,
          emr_date.edwcr_date_driver - INTERVAL 90 DAY AS hospemr_date_driver
   FROM
     (SELECT info.cancer_patient_driver_sk,
             info.network_mnemonic_cs,
             max(info.patient_market_urn_text) AS patient_market_urn_text,
             max(info.medical_record_num) AS medical_record_num,
             max(info.empi_text) AS empi_text,
             max(info.coid) AS coid,
             max(info.datesource) AS datesource,
             min(info.date_driver) AS edwcr_date_driver
      FROM
        (SELECT dates.cancer_patient_driver_sk,
                dates.network_mnemonic_cs,
                max(dates.patient_market_urn_text) AS patient_market_urn_text,
                min(dates.datesourceorder) AS mindatesource
         FROM
           (SELECT t2.cancer_patient_driver_sk,
                   t2.network_mnemonic_cs,
                   max(t2.patient_market_urn_text) AS patient_market_urn_text,
                   max(t2.medical_record_num) AS medical_record_num,
                   max(t2.empi_text) AS empi_text,
                   'Patient_ID' AS datesource,
                   3 AS datesourceorder,
                   min(t1.user_action_date_time) AS date_driver
            FROM {{ params.param_cr_base_views_dataset_name }}.cancer_patient_id_output AS t1
            INNER JOIN {{ params.param_cr_core_dataset_name }}.cancer_patient_driver AS t2
            FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON t1.patient_dw_id = t2.cp_patient_id
            WHERE upper(rtrim(t1.user_action_desc)) = 'CONFIRM'
              AND upper(rtrim(t1.message_flag_code)) <> 'RAD'
            GROUP BY 1,
                     2,
                     upper(t2.patient_market_urn_text),
                     upper(t2.medical_record_num),
                     upper(t2.empi_text)
            UNION DISTINCT SELECT t1.cancer_patient_driver_sk,
                                  t1.network_mnemonic_cs,
                                  max(t1.patient_market_urn_text) AS patient_market_urn_text,
                                  max(t1.medical_record_num) AS medical_record_num,
                                  max(t1.empi_text) AS empi_text,
                                  'Nav_Diagno' AS datesource,
                                  2 AS datesourceorder,
                                  CAST(min(t3.diagnosis_date) AS DATETIME) AS date_driver
            FROM {{ params.param_cr_base_views_dataset_name }}.cancer_patient_driver AS t1
            INNER JOIN {{ params.param_cr_core_dataset_name }}.cn_patient AS t2
            FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON t1.cn_patient_id = t2.nav_patient_id
            INNER JOIN {{ params.param_cr_core_dataset_name }}.cn_patient_diagnosis AS t3
            FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON t2.nav_patient_id = t3.nav_patient_id
            WHERE t3.diagnosis_date > DATE '1900-01-01'
            GROUP BY 1,
                     2,
                     upper(t1.patient_market_urn_text),
                     upper(t1.medical_record_num),
                     upper(t1.empi_text)
            UNION DISTINCT SELECT t1.cancer_patient_driver_sk,
                                  t1.network_mnemonic_cs,
                                  max(t1.patient_market_urn_text) AS patient_market_urn_text,
                                  max(t1.medical_record_num) AS medical_record_num,
                                  max(t1.empi_text) AS empi_text,
                                  'NAV_CREATE' AS datesource,
                                  4 AS datesourceorder,
                                  CAST(min(t2.nav_create_date) AS DATETIME) AS date_driver
            FROM {{ params.param_cr_base_views_dataset_name }}.cancer_patient_driver AS t1
            INNER JOIN {{ params.param_cr_core_dataset_name }}.cn_patient AS t2
            FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON t1.cn_patient_id = t2.nav_patient_id
            WHERE t2.nav_create_date > DATE '1900-01-01'
            GROUP BY 1,
                     2,
                     upper(t1.patient_market_urn_text),
                     upper(t1.medical_record_num),
                     upper(t1.empi_text)
            UNION DISTINCT SELECT t1.cancer_patient_driver_sk,
                                  t1.network_mnemonic_cs,
                                  max(t1.patient_market_urn_text) AS patient_market_urn_text,
                                  max(t1.medical_record_num) AS medical_record_num,
                                  max(t1.empi_text) AS empi_text,
                                  'CR_DIAGNOS' AS datesource,
                                  1 AS datesourceorder,
                                  CAST(min(t2.diagnosis_date) AS DATETIME) AS date_driver
            FROM {{ params.param_cr_base_views_dataset_name }}.cancer_patient_driver AS t1
            INNER JOIN {{ params.param_cr_core_dataset_name }}.cr_patient_diagnosis_detail AS t2
            FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON t1.cr_patient_id = t2.cr_patient_id
            AND t2.diagnosis_date IS NOT NULL
            WHERE t2.diagnosis_date > DATE '1900-01-01'
            GROUP BY 1,
                     2,
                     upper(t1.patient_market_urn_text),
                     upper(t1.medical_record_num),
                     upper(t1.empi_text)
            UNION DISTINCT SELECT t1.cancer_patient_driver_sk,
                                  t1.network_mnemonic_cs,
                                  max(t1.patient_market_urn_text) AS patient_market_urn_text,
                                  max(t1.medical_record_num) AS medical_record_num,
                                  max(t1.empi_text) AS empi_text,
                                  'CR_ADMISSI' AS datesource,
                                  5 AS datesourceorder,
                                  CAST(min(t2.admission_date) AS DATETIME) AS date_driver
            FROM {{ params.param_cr_base_views_dataset_name }}.cancer_patient_driver AS t1
            INNER JOIN {{ params.param_cr_core_dataset_name }}.cr_patient_tumor AS t2
            FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON t1.cr_patient_id = t2.cr_patient_id
            AND t2.admission_date IS NOT NULL
            WHERE t2.admission_date > DATE '1900-01-01'
            GROUP BY 1,
                     2,
                     upper(t1.patient_market_urn_text),
                     upper(t1.medical_record_num),
                     upper(t1.empi_text)) AS dates
         GROUP BY 1,
                  2,
                  upper(dates.patient_market_urn_text)) AS date_source
      INNER JOIN
        (SELECT t2.cancer_patient_driver_sk,
                t2.network_mnemonic_cs,
                max(t2.patient_market_urn_text) AS patient_market_urn_text,
                max(t2.medical_record_num) AS medical_record_num,
                max(t2.empi_text) AS empi_text,
                max(t2.coid) AS coid,
                'Patient_ID' AS datesource,
                3 AS datesourceorder,
                min(t1.user_action_date_time) AS date_driver
         FROM {{ params.param_cr_base_views_dataset_name }}.cancer_patient_id_output AS t1
         INNER JOIN {{ params.param_cr_core_dataset_name }}.cancer_patient_driver AS t2
         FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON t1.patient_dw_id = t2.cp_patient_id
         WHERE upper(rtrim(t1.user_action_desc)) = 'CONFIRM'
           AND upper(rtrim(t1.message_flag_code)) <> 'RAD'
         GROUP BY 1,
                  2,
                  upper(t2.patient_market_urn_text),
                  upper(t2.medical_record_num),
                  upper(t2.empi_text),
                  upper(t2.coid)
         UNION DISTINCT SELECT t1.cancer_patient_driver_sk,
                               t1.network_mnemonic_cs,
                               max(t1.patient_market_urn_text) AS patient_market_urn_text,
                               max(t1.medical_record_num) AS medical_record_num,
                               max(t1.empi_text) AS empi_text,
                               max(t1.coid) AS coid,
                               'Nav_Diagno' AS datesource,
                               2 AS datesourceorder,
                               CAST(min(t3.diagnosis_date) AS DATETIME) AS date_driver
         FROM {{ params.param_cr_base_views_dataset_name }}.cancer_patient_driver AS t1
         INNER JOIN {{ params.param_cr_core_dataset_name }}.cn_patient AS t2
         FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON t1.cn_patient_id = t2.nav_patient_id
         INNER JOIN {{ params.param_cr_core_dataset_name }}.cn_patient_diagnosis AS t3
         FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON t2.nav_patient_id = t3.nav_patient_id
         WHERE t3.diagnosis_date > DATE '1900-01-01'
         GROUP BY 1,
                  2,
                  upper(t1.patient_market_urn_text),
                  upper(t1.medical_record_num),
                  upper(t1.empi_text),
                  upper(t1.coid)
         UNION DISTINCT SELECT t1.cancer_patient_driver_sk,
                               t1.network_mnemonic_cs,
                               max(t1.patient_market_urn_text) AS patient_market_urn_text,
                               max(t1.medical_record_num) AS medical_record_num,
                               max(t1.empi_text) AS empi_text,
                               max(t1.coid) AS coid,
                               'Nav_Create' AS datesource,
                               4 AS datesourceorder,
                               CAST(min(t2.nav_create_date) AS DATETIME) AS date_driver
         FROM {{ params.param_cr_base_views_dataset_name }}.cancer_patient_driver AS t1
         INNER JOIN {{ params.param_cr_core_dataset_name }}.cn_patient AS t2
         FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON t1.cn_patient_id = t2.nav_patient_id
         WHERE t2.nav_create_date > DATE '1900-01-01'
         GROUP BY 1,
                  2,
                  upper(t1.patient_market_urn_text),
                  upper(t1.medical_record_num),
                  upper(t1.empi_text),
                  upper(t1.coid)
         UNION DISTINCT SELECT t1.cancer_patient_driver_sk,
                               t1.network_mnemonic_cs,
                               max(t1.patient_market_urn_text) AS patient_market_urn_text,
                               max(t1.medical_record_num) AS medical_record_num,
                               max(t1.empi_text) AS empi_text,
                               max(t1.coid) AS coid,
                               'CR_Diagnos' AS datesource,
                               1 AS datesourceorder,
                               CAST(min(t2.diagnosis_date) AS DATETIME) AS date_driver
         FROM {{ params.param_cr_base_views_dataset_name }}.cancer_patient_driver AS t1
         INNER JOIN {{ params.param_cr_core_dataset_name }}.cr_patient_diagnosis_detail AS t2
         FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON t1.cr_patient_id = t2.cr_patient_id
         AND t2.diagnosis_date IS NOT NULL
         WHERE t2.diagnosis_date > DATE '1900-01-01'
         GROUP BY 1,
                  2,
                  upper(t1.patient_market_urn_text),
                  upper(t1.medical_record_num),
                  upper(t1.empi_text),
                  upper(t1.coid)
         UNION DISTINCT SELECT t1.cancer_patient_driver_sk,
                               t1.network_mnemonic_cs,
                               max(t1.patient_market_urn_text) AS patient_market_urn_text,
                               max(t1.medical_record_num) AS medical_record_num,
                               max(t1.empi_text) AS empi_text,
                               max(t1.coid) AS coid,
                               'CR_Admissi' AS datesource,
                               5 AS datesourceorder,
                               CAST(min(t2.admission_date) AS DATETIME) AS date_driver
         FROM {{ params.param_cr_base_views_dataset_name }}.cancer_patient_driver AS t1
         INNER JOIN {{ params.param_cr_core_dataset_name }}.cr_patient_tumor AS t2
         FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON t1.cr_patient_id = t2.cr_patient_id
         AND t2.admission_date IS NOT NULL
         WHERE t2.admission_date > DATE '1900-01-01'
         GROUP BY 1,
                  2,
                  upper(t1.patient_market_urn_text),
                  upper(t1.medical_record_num),
                  upper(t1.empi_text),
                  upper(t1.coid)) AS info ON date_source.cancer_patient_driver_sk = info.cancer_patient_driver_sk
      AND date_source.mindatesource = info.datesourceorder
      GROUP BY 1,
               2,
               upper(info.patient_market_urn_text),
               upper(info.medical_record_num),
               upper(info.empi_text),
               upper(info.coid),
               upper(info.datesource)) AS emr_date)
SELECT concat(format('%20d', count(*)), ',\t') AS source_string
FROM
  (SELECT a.cancer_patient_driver_sk,
          a.patient_dw_id,
          a.edwcr_date_driver AS cancer_diagnosis_date,
          a.hospemr_date_driver AS cancer_diagnosis_90_day_prior_date,
          datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
   FROM
     (SELECT fe.patient_dw_id,
             fe.patient_market_urn,
             fe.medical_record_num,
             sc.cancer_patient_driver_sk,
             fe.admission_date_time,
             sc.edwcr_date_driver,
             sc.hospemr_date_driver
      FROM {{ params.param_cr_base_views_dataset_name }}.fact_encounter AS fe
      INNER JOIN sc ON upper(rtrim(fe.medical_record_num)) = upper(rtrim(sc.medical_record_num))
      AND upper(rtrim(fe.coid)) = upper(rtrim(sc.coid))
      WHERE fe.admission_date_time >= sc.hospemr_date_driver ) AS a
   UNION ALL SELECT b.cancer_patient_driver_sk,
                    b.patient_dw_id,
                    b.edwcr_date_driver AS cancer_diagnosis_date,
                    b.hospemr_date_driver AS cancer_diagnosis_90_day_prior_date,
                    datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
   FROM
     (SELECT fe.patient_dw_id,
             fe.patient_market_urn,
             fe.medical_record_num,
             sc.cancer_patient_driver_sk,
             fe.admission_date_time,
             sc.edwcr_date_driver,
             sc.hospemr_date_driver
      FROM {{ params.param_cr_base_views_dataset_name }}.fact_encounter AS fe
      INNER JOIN `hca-hin-dev-cur-ops`.edw_pub_views.clinical_facility AS cf ON upper(rtrim(fe.coid)) = upper(rtrim(cf.coid))
      AND upper(rtrim(cf.facility_active_ind)) = 'Y'
      INNER JOIN sc ON upper(rtrim(fe.patient_market_urn)) = upper(rtrim(sc.patient_market_urn_text))
      AND rtrim(cf.network_mnemonic_cs) = rtrim(sc.network_mnemonic_cs)
      WHERE fe.admission_date_time >= sc.hospemr_date_driver ) AS b
   WHERE b.patient_dw_id + b.cancer_patient_driver_sk NOT IN
       (SELECT a.patient_dw_id + a.cancer_patient_driver_sk
        FROM
          (SELECT fe.patient_dw_id,
                  fe.patient_market_urn,
                  fe.medical_record_num,
                  sc.cancer_patient_driver_sk,
                  fe.admission_date_time,
                  sc.edwcr_date_driver,
                  sc.hospemr_date_driver
           FROM {{ params.param_cr_base_views_dataset_name }}.fact_encounter AS fe
           INNER JOIN sc ON upper(rtrim(fe.medical_record_num)) = upper(rtrim(sc.medical_record_num))
           AND upper(rtrim(fe.coid)) = upper(rtrim(sc.coid))
           WHERE fe.admission_date_time >= sc.hospemr_date_driver ) AS a) ) AS a)
  );

  SET act_values_list =
  (
  SELECT SPLIT(SOURCE_STRING,',') values_list
  FROM (SELECT format('%20d', count(*)) AS source_string
FROM {{ params.param_cr_core_dataset_name }}.cr_patient_date_driver)
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
