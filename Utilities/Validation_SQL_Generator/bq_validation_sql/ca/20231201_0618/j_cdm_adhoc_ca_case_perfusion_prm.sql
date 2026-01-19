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

SET srcdataset_id = (SELECT arr[offset(1)] FROM (SELECT SPLIT({{ params.param_ca_stage_dataset_name }} , '.') AS arr));

SET srctablename = concat(srcdataset_id, '.','' ); -- This needs to be added

SET tgtdataset_id = (SELECT arr[offset(1)] FROM (SELECT SPLIT({{ params.param_ca_core_dataset_name }} , '.') AS arr));

SET tgttablename =concat(tgtdataset_id, '.', @p_targettable_name);

SET audit_type ='RECORD_COUNT';

SET tableload_start_time = @p_tableload_start_time;

SET tableload_end_time = @p_tableload_end_time;

SET tableload_run_time = CAST((tableload_end_time - tableload_start_time) AS STRING);

SET job_name = @p_job_name;

SET audit_time = current_ts;

SET tolerance_percent = 0;

SET
(src_rec_count) = 
(
SELECT count(*)
FROM
  (SELECT DISTINCT a.*
   FROM
     (SELECT NULL AS case_perfusion_sk,
             a_0.patient_case_sk AS patient_case_sk,
             csrv.server_sk AS server_sk,
             stg.perfusionnumber AS source_case_perfusion_id,
             stg.cpbtm AS cpb_bypass_time_minute_num,
             stg.xclamptm AS cross_clamp_time_minute_num,
             stg.dhcatm AS circulatory_arrest_time_minute_num,
             stg.cooltime AS cool_time_minute_num,
             stg.rewarmtime AS rewarm_time_minute_num,
             stg.cperfutil AS cperf_utilized_id,
             stg.cperftime AS cperf_utilized_time_minute_num,
             stg.cperfcaninn AS cperf_inn_art_cann_site_id,
             stg.cperfcanrsub AS cperf_right_subcl_cann_site_id,
             stg.cperfcanrax AS cperf_right_axil_art_cann_site_id,
             stg.cperfcanrcar AS cperf_right_car_art_cann_site_id,
             stg.cperfcanlcar AS cperf_left_car_art_cann_site_id,
             stg.cperfcansvc AS cperf_svc_cann_site_id,
             stg.cperfper AS cperf_period_num,
             stg.cperfflow AS cperf_flow_rt_id,
             stg.abldgasmgt AS abgm_cooling_id,
             stg.hctpricirca AS hct_prior_cir_arrest_amt,
             stg.cplegiaadmin AS cplegia_admin_id,
             stg.cplegiadose AS cplegia_dose_num,
             stg.cplegiaratiobs AS cplegia_bs_ratio_num,
             stg.cplegiaratiocs AS cplegia_solution_ratio_num,
             stg.cpleginrtaar AS cplegia_aar_in_route_num,
             stg.cpleginrtarco AS cplegia_arco_in_route_num,
             stg.cpleginrtalco AS cplegia_alco_in_route_num,
             stg.cpleginrtrcs AS cplegia_rcs_in_route_num,
             stg.cplegsubrtaar AS cplegia_aar_subq_route_num,
             stg.cplegsubrtarco AS cplegia_arco_subq_route_num,
             stg.cplegsubrtalco AS cplegia_alco_subq_route_num,
             stg.cplegsubrtrcs AS cplegia_rcs_subq_route_num,
             stg.cplegtemp AS cplegia_tmp_num,
             stg.cplegtotalvol AS cplegia_total_volume_num,
             stg.lngmyoiscint AS lmii_minute_num,
             stg.cplegsol AS cplegia_solution_num,
             stg.lowhct AS lowest_hct_cpb_num,
             stg.cerebralflowtype AS cerebral_flow_type_id,
             stg.cpbprimed AS cpb_blood_prime_num,
             stg.cplegiadeliv AS cplegia_delivery_id,
             stg.cplegiatype AS cplegia_type_id,
             stg.hctfirst AS first_hct_cpb_amt,
             stg.hctlast AS last_hct_cpb_amt,
             stg.hctpost AS hct_post_cpb_amt,
             stg.ultrafiltration AS ultrafiltration_performed_id,
             stg.createdate AS source_create_date_time,
             stg.lastupdate AS source_last_update_date_time,
             stg.updatedby AS updated_by_3_4_id,
             'C' AS source_system_code,
             timestamp_trunc(current_timestamp(), SECOND) AS dw_last_update_date_time
      FROM {{ params.param_clinical_cdm_stage_dataset_name }}.cardioaccess_perfusion_stg AS stg
      LEFT OUTER JOIN
        (SELECT c.source_patient_case_num,
                s.server_name,
                c.patient_case_sk
         FROM {{ params.param_clinical_cdm_core_dataset_name }}.ca_patient_case AS c
         FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central')
         INNER JOIN {{ params.param_clinical_cdm_core_dataset_name }}.ca_server AS s
         FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON c.server_sk = s.server_sk) AS a_0 ON stg.casenumber = a_0.source_patient_case_num
      AND upper(stg.full_server_nm) = upper(a_0.server_name)
      INNER JOIN {{ params.param_clinical_cdm_core_dataset_name }}.ca_server AS csrv
      FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON upper(stg.full_server_nm) = upper(csrv.server_name)
      LEFT OUTER JOIN {{ params.param_clinical_cdm_core_dataset_name }}.ca_case_perfusion AS ch
      FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON ch.server_sk = csrv.server_sk
      AND ch.source_case_perfusion_id = stg.perfusionnumber
      WHERE ch.server_sk IS NULL
        AND ch.source_case_perfusion_id IS NULL ) AS a) AS b
);

SET
(tgt_rec_count) =
(
SELECT coalesce(count(*), 0)
FROM {{ params.param_clinical_cdm_core_dataset_name }}.ca_case_perfusion
WHERE ca_case_perfusion.dw_last_update_date_time >=
    (SELECT max(etl_job_run.job_start_date_time) AS job_start_date_time
     FROM `hca-hin-dev-cur-clinical`.edwcdm_ac.etl_job_run
     WHERE upper(etl_job_run.job_name) = 'J_CDM_ADHOC_CA_CASE_PERFUSION' )
);

SET
difference = CASE 
              WHEN src_rec_count <> 0 Then CAST(((ABS(tgt_rec_count - src_rec_count)/src_rec_count) * 100) AS INT64)
              WHEN src_rec_count =0 AND tgt_rec_count = 0 Then 0
              ELSE tgt_rec_count
              END;


SET
audit_status = CASE
    WHEN difference <= tolerance_percent THEN "PASS"
    ELSE "FAIL"
END;

##Insert statement
INSERT INTO
 {{ params.param_ca_audit_dataset_name }}.audit_control
VALUES
  (GENERATE_UUID(), cast(srctableid AS int64), sourcesysnm, srctablename, tgttablename, audit_type, 
  src_rec_count, tgt_rec_count, control2_src_rowcount, control2_tgt_rowcount, control3_src_rowcount, control3_tgt_rowcount,
  cast(tableload_start_time AS DATETIME), cast(tableload_end_time AS DATETIME),
  tableload_run_time, job_name, audit_time, audit_status );
END;
