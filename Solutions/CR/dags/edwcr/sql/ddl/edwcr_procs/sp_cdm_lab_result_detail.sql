-- Translation time: 2024-04-11T17:46:05.729460Z
-- Translation job ID: 292d7f41-1de7-4c69-ae5f-c8272f16f7a3
-- Source: eim-ops-cs-datamig-dev-0002/cr_conversion/6O3Ce4/input/sp_cdm_lab_result_detail.sql
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE PROCEDURE `{{ params.param_cr_procs_dataset_name }}`.sp_cdm_lab_result_detail()
  BEGIN
    -- #####################################################################################
    -- #                                                                                   																											  #
    -- # Target Table - EDWCR.CDM_Lab_Result_Detail                                                  																  #
    -- #                                                                                   																											  #
    -- # CHANGE CONTROL:                                                                   																						      #
    -- #                                                                                   																											  #
    -- # DATE          Developer     Change Comment                                        																					  #
    -- # 03/25/2019    J.Huertas       Initial Version                                     																						  #
    -- #                                                                                   																											  #
    -- #                                                                                   																											  #
    -- # Created as part of Sarah Cannon Project                                          																						  #
    -- #                                                                                   																											  #
    -- #                     															  																												  #
    -- #                                                                                   																											  #
    -- #   Loads the CDM_Lab_Result_Detail tables from data available in EDWCDM. This table contains lab tests, their results, 	  #
    -- #   and range information for a result.	  																		  															  #
    -- #   																			                              																					  #
    -- #                                                                                   																											  #
    -- #                                                                                  																												  #
    -- #####################################################################################
    -- FIRST STMT
    DECLARE qb_stmt STRING;
    -- Set Queryband
    -- SET QB_Stmt = 'SET QUERY_BAND = ''App=CDM_Sarah_Cannon_ETL;Job=CDM_Lab_Result_Detail;'' FOR SESSION;';
    -- CALL DBC.SysExecSQL(QB_Stmt);
    --  Delete prior data for Attribute
    TRUNCATE TABLE `{{ params.param_cr_core_dataset_name }}`.cdm_lab_result_detail;
    --   Load Attribute Data
    INSERT INTO `{{ params.param_cr_core_dataset_name }}`.cdm_lab_result_detail (clinical_finding_sk, patient_dw_id, company_code, coid, lab_test_type_ref_code, lab_test_type_desc, lab_test_id_text, lab_test_subid_text, lab_test_collect_ts, lab_test_specimen_received_ts, lab_test_reported_ts, lab_test_result_status_ts, lab_test_result_status_ref_code, lab_test_value_numeric_ind, lab_test_value_unit_type_code, lab_test_value_text, lab_test_value_num, ref_range_low_text, ref_range_high_text, ref_range_text, abnormal_flag_name, source_system_original_code, source_system_code, dw_last_update_date_time)
      SELECT DISTINCT
          rslt.clnc_fnd_sk AS clinical_finding_sk,
          rslt.patient_dw_id,
          rslt.company_code,
          rslt.coid,
          TRIM(rslt.lab_tst_type_ref_cd) AS lab_test_type_ref_code,
          substr(CASE
             upper(TRIM(rslt.lab_tst_type_ref_cd))
            WHEN 'LAB' THEN 'Lab'
            WHEN 'MIC' THEN 'Microbiology'
            ELSE ''
          END, 1, 32) AS lab_test_type_desc,
          TRIM(rslt.lab_tst_id_txt) AS lab_test_id_text,
          TRIM(rslt.lab_tst_subid_txt) AS lab_test_subid_text,
          rslt.lab_tst_coll_ts AS lab_test_collect_ts,
          rslt.lab_tst_spcmn_rcvd_ts AS lab_test_specimen_received_ts,
          rslt.lab_tst_rptd_ts AS lab_test_reported_ts,
          rslt.lab_tst_rslt_sts_ts AS lab_test_result_status_ts,
          TRIM(rslt.lab_tst_rslt_sts_ref_cd) AS lab_test_result_status_ref_code,
          TRIM(rslt.lab_tst_val_nmrc_ind) AS lab_test_value_numeric_ind,
          rslt.lab_tst_val_unt_type_cd AS lab_test_value_unit_type_code,
          RTRIM(substr(TRIM(rslt.lab_tst_val_txt), 1, 255)) AS lab_test_value_text,
          rslt.lab_tst_val_num AS lab_test_value_num,
          TRIM(rslt.ref_rng_low) AS ref_range_low_text,
          TRIM(rslt.ref_rng_hi) AS ref_range_high_text,
          TRIM(rslt.ref_rng_txt) AS ref_range_text,
          TRIM(rslt.abnrml_flg_nm) AS abnormal_flag_name,
          RTRIM(substr(TRIM(rslt.src_sys_ref_cd), 1, 60)) AS source_system_original_code,
          'C' AS source_system_code,
          datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
        FROM
          `{{ params.param_cr_base_views_dataset_name }}`.lab_tst_rslt_dtl AS rslt
          INNER JOIN (
            SELECT DISTINCT
                cr_patient_date_driver.patient_dw_id
              FROM
                `{{ params.param_cr_base_views_dataset_name }}`.cr_patient_date_driver
          ) AS driver ON driver.patient_dw_id = rslt.patient_dw_id
           AND rslt.vld_to_ts = DATETIME '9999-12-31 00:00:00'
    ;
  END;
-- LAST STMT
-- Collect Stats
-- CALL DBADMIN_PROCS.collect_stats_table('EDWCR','CDM_Lab_Result_Detail');
-- Clear Queryband
-- SET QB_Stmt = 'SET QUERY_BAND = NONE FOR SESSION;';
-- CALL DBC.SysExecSQL(QB_Stmt);
