-- Translation time: 2024-04-11T17:46:05.729460Z
-- Translation job ID: 292d7f41-1de7-4c69-ae5f-c8272f16f7a3
-- Source: eim-ops-cs-datamig-dev-0002/cr_conversion/6O3Ce4/input/sp_cdm_patient_diagnosis.sql
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE PROCEDURE `{{ params.param_cr_procs_dataset_name }}`.sp_cdm_patient_diagnosis()
  BEGIN
    -- #####################################################################################
    -- #                                                                                   																											  #
    -- # Target Table - EDWCR.CDM_Patient_Diagnosis                                                  																  #
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
    -- #   Loads the CDM_Patient_Diagnosis tables from data available in EDWCDM.	Contains all diagnoses at the encounter        #
    -- #   level.	      																		  																									  #
    -- #  																		                                     																					  #
    -- #                                                                                   																											  #
    -- #                                                                                  																												  #
    -- #####################################################################################
    -- FIRST STMT
    DECLARE qb_stmt STRING;
    -- Set Queryband
    -- SET QB_Stmt = 'SET QUERY_BAND = ''App=CDM_Sarah_Cannon_ETL;Job=CDM_Patient_Diagnosis;'' FOR SESSION;';
    -- CALL DBC.SysExecSQL(QB_Stmt);
    --  Delete prior data for Attribute
    TRUNCATE TABLE `{{ params.param_cr_core_dataset_name }}`.cdm_patient_diagnosis;
    --   Load Attribute Data
    INSERT INTO `{{ params.param_cr_core_dataset_name }}`.cdm_patient_diagnosis (patient_dw_id, diagnosis_cycle_code, diagnosis_mapped_code, diagnosis_code, diagnosis_type_code, diagnosis_type_code_desc, coid, company_code, pat_acct_num, diagnosis_rank_num, diagnosis_short_desc, cancer_diagnosis_ind, source_system_code, dw_last_update_date_time)
      SELECT DISTINCT
          pd.patient_dw_id,
          TRIM(pd.diag_cycle_code) AS diagnosis_cycle_code,
          TRIM(pd.diag_mapped_code) AS diagnosis_mapped_code,
          TRIM(pd.diag_code) AS diagnosis_code,
          TRIM(pd.diag_type_code) AS diagnosis_type_code,
          substr(CASE
             upper(TRIM(pd.diag_type_code))
            WHEN '9' THEN 'ICD-09'
            WHEN '0' THEN 'ICD-10'
            ELSE ''
          END, 1, 25) AS diagnosis_type_code_desc,
          pd.coid,
          pd.company_code,
          pd.pat_acct_num,
          pd.diag_rank_num AS diagnosis_rank_num,
          TRIM(refdiag.diagnosis_short_desc),
          CASE
            WHEN refcancerdiag.diagnosis_code IS NOT NULL THEN 'Y'
            ELSE 'N'
          END AS cancer_diagnosis_ind,
          'C' AS source_system_code,
          datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
        FROM
          `{{ params.param_cr_base_views_dataset_name }}`.patient_diagnosis AS pd
          INNER JOIN (
            SELECT DISTINCT
                cr_patient_date_driver.patient_dw_id
              FROM
                `{{ params.param_cr_base_views_dataset_name }}`.cr_patient_date_driver
          ) AS cr_driver ON pd.patient_dw_id = cr_driver.patient_dw_id
          LEFT OUTER JOIN `{{ params.param_cr_base_views_dataset_name }}`.ref_diagnosis AS refdiag ON upper(TRIM(pd.diag_code)) = upper(TRIM(refdiag.diag_code))
           AND upper(TRIM(pd.diag_type_code)) = upper(TRIM(refdiag.diag_type_code))
          LEFT OUTER JOIN `{{ params.param_cr_base_views_dataset_name }}`.ref_cancer_diagnosis AS refcancerdiag ON upper(TRIM(pd.diag_code)) = upper(TRIM(refcancerdiag.diagnosis_code))
        WHERE upper(TRIM(pd.diag_cycle_code)) = 'F'
         AND upper(TRIM(pd.diag_mapped_code)) = 'N'
         AND upper(TRIM(pd.company_code)) = 'H'
    ;
  END;
-- LAST STMT
-- Collect Stats
-- CALL DBADMIN_PROCS.collect_stats_table('EDWCR','CDM_Patient_Diagnosis');
-- Clear Queryband
-- SET QB_Stmt = 'SET QUERY_BAND = NONE FOR SESSION;';
-- CALL DBC.SysExecSQL(QB_Stmt);
