-- Translation time: 2023-12-01T12:21:28.647193Z
-- Translation job ID: 4144eabf-8284-495a-81e3-de22d755910b
-- Source: eim-clin-pdoc-ccda-dev-0001/ca_bulk_conversion_validation/20231201_0618/input/exp/j_cdm_adhoc_ca_case_perfusion_prm.sql
-- Translated from: Teradata
-- Translated to: BigQuery

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