-- Translation time: 2024-04-11T17:46:05.729460Z
-- Translation job ID: 292d7f41-1de7-4c69-ae5f-c8272f16f7a3
-- Source: eim-ops-cs-datamig-dev-0002/cr_conversion/6O3Ce4/input/sp_cdm_patient_history.sql
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE PROCEDURE `{{ params.param_cr_procs_dataset_name }}`.sp_cdm_patient_history()
  BEGIN
    -- #####################################################################################
    -- #                                                                                   																											  #
    -- # Target Table - EDWCR.CDM_Patient_History                                                  																          #
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
    -- #   Loads the CDM_Patient_History table from data available in EDWCDM. Contains the information about the patient  		  #
    -- #   smoking status.  																																									  #
    -- #                                   																					  																		  #
    -- #                                                                                   																											  #
    -- #                                                                                  																												  #
    -- #####################################################################################
    -- FIRST STMT
    DECLARE qb_stmt STRING;
    -- Set Queryband
    -- SET QB_Stmt = 'SET QUERY_BAND = ''App=CDM_Sarah_Cannon_ETL;Job=CDM_Patient_History;'' FOR SESSION;';
    -- CALL DBC.SysExecSQL(QB_Stmt);
    --  Delete prior data for Attribute
    TRUNCATE TABLE `{{ params.param_cr_core_dataset_name }}`.cdm_patient_history;
    --   Load Attribute Data
    INSERT INTO `{{ params.param_cr_core_dataset_name }}`.cdm_patient_history (clinical_patient_query_sid, patient_dw_id, coid, company_code, pat_acct_num, smoking_status_id, smoking_status_desc, source_system_code, dw_last_update_date_time)
      SELECT DISTINCT
          pq.clinical_patient_query_sid,
          pq.patient_dw_id,
          pq.coid,
          pq.company_code,
          pq.pat_acct_num,
          rss.smoking_status_id,
          TRIM(rss.smoking_status_desc),
          'C' AS source_system_code,
          datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
        FROM
          `{{ params.param_cr_base_views_dataset_name }}`.clinical_patient_query AS pq
          INNER JOIN (
            SELECT DISTINCT
                cr_patient_date_driver.patient_dw_id
              FROM
                `{{ params.param_cr_base_views_dataset_name }}`.cr_patient_date_driver
          ) AS a ON a.patient_dw_id = pq.patient_dw_id
          INNER JOIN `{{ params.param_cr_base_views_dataset_name }}`.ref_parm_facility_dtl AS pd ON upper(TRIM(pq.company_code)) = upper(TRIM(pd.company_code))
           AND upper(TRIM(pq.coid)) = upper(TRIM(pd.coid))
           AND TRIM(pq.query_mnemonic_cs) = TRIM(pd.parameter_mnemonic_cs)
           AND pd.parameter_code IN(
            165, 8
          )
          INNER JOIN --  165 is the new parameter code for Smoking. 8 is the old parameter code to capture historical patients
          -- AND pd. parameter_type_code = 'MU3SMKQRY'
          -- AND pd.Parameter_Code = 165
          `{{ params.param_cr_base_views_dataset_name }}`.ref_smoking_status AS rss ON CAST(`hca-hin-dev-cur-ops`.bqutil_fns.cw_td_normalize_number(coalesce(pq.query_response_text, format('%4d', 0))) as INT64) = rss.smoking_status_id
           AND upper(TRIM(pq.query_response_text)) IN(
            '1', '2', '3', '4', '5', '6', '7', '8', '9', '0'
          )
        QUALIFY row_number() OVER (PARTITION BY pq.clinical_patient_query_sid ORDER BY pd.parameter_code DESC) = 1
    ;
  END;
-- LAST STMT
-- Collect Stats
-- CALL DBADMIN_PROCS.collect_stats_table('EDWCR','CDM_Patient_History');
-- Clear Queryband
-- SET QB_Stmt = 'SET QUERY_BAND = NONE FOR SESSION;';
-- CALL DBC.SysExecSQL(QB_Stmt);
