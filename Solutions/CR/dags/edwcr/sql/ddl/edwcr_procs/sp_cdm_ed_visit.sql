-- Translation time: 2024-04-11T17:46:05.729460Z
-- Translation job ID: 292d7f41-1de7-4c69-ae5f-c8272f16f7a3
-- Source: eim-ops-cs-datamig-dev-0002/cr_conversion/6O3Ce4/input/sp_cdm_ed_visit.sql
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE PROCEDURE `{{ params.param_cr_procs_dataset_name }}`.sp_cdm_ed_visit()
  BEGIN
    -- #####################################################################################
    -- #                                                                                   																											  #
    -- # Target Table - EDWCR.CDM_ED_Visit                                                  																                  #
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
    -- #   Loads the CDM_ED_Visit table from data available in EDWCDM. This table contains the emergency department 			  #
    -- #   data at the patient level.     																		  																			  #
    -- #                               																					  																			  #
    -- #                                                                                   																											  #
    -- #                                                                                  																												  #
    -- #####################################################################################
    -- FIRST STMT
    DECLARE qb_stmt STRING;
    -- Set Queryband
    -- SET QB_Stmt = 'SET QUERY_BAND = ''App=CDM_Sarah_Cannon_ETL;Job=CDM_ED_Visit;'' FOR SESSION;';
    -- CALL DBC.SysExecSQL(QB_Stmt);
    --  Delete prior data for Attribute
    TRUNCATE TABLE `{{ params.param_cr_core_dataset_name }}`.cdm_ed_visit;
    --   Load Attribute Data
    INSERT INTO `{{ params.param_cr_core_dataset_name }}`.cdm_ed_visit (patient_dw_id, coid, company_code, pat_acct_num, reason_for_visit_text, chief_complaint_query_mnemonic_cs, ed_complaint_desc, arrival_date_time, depart_date_time, admit_date_time, admit_ind, source_system_code, dw_last_update_date_time)
      SELECT DISTINCT
          pd.patient_dw_id,
          pd.coid,
          pd.company_code,
          pd.pat_acct_num,
          TRIM(pd.reason_for_visit_txt) AS reason_for_visit_text,
          TRIM(pd.chief_complaint_query_mnemonic_cs),
          TRIM(refed.ed_complaint_desc),
          pd.arrival_date_time,
          -- Date of ED Admission/The date and time a patient first arrives to ED
          pd.depart_date_time,
          pd.admit_date_time,
          -- Date Admitted from ED TO Hospital/The date time that someone becomes an inpatient
          CASE
            WHEN pd.admit_date_time IS NULL THEN 'N'
            ELSE 'Y'
          END AS admit_ind,
          -- Admitted from ED to Hospital (Indicator)
          'C' AS source_system_code,
          datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
        FROM
          `{{ params.param_cr_base_views_dataset_name }}`.fact_ed_patient_detail AS pd
          INNER JOIN (
            SELECT DISTINCT
                cr_patient_date_driver.patient_dw_id
              FROM
                `{{ params.param_cr_base_views_dataset_name }}`.cr_patient_date_driver
          ) AS cr_driver ON pd.patient_dw_id = cr_driver.patient_dw_id
          LEFT OUTER JOIN `{{ params.param_cr_base_views_dataset_name }}`.ref_ed_complaint AS refed ON upper(TRIM(pd.company_code)) = upper(TRIM(refed.company_code))
           AND upper(TRIM(pd.coid)) = upper(TRIM(refed.coid))
           AND TRIM(pd.chief_complaint_query_mnemonic_cs) = TRIM(refed.ed_complaint_code)
           AND upper(TRIM(refed.active_ind)) = 'Y'
    ;
  END;
-- LAST STMT
-- Collect Stats
-- CALL DBADMIN_PROCS.collect_stats_table('EDWCR','CDM_ED_Visit');
-- Clear Queryband
-- SET QB_Stmt = 'SET QUERY_BAND = NONE FOR SESSION;';
-- CALL DBC.SysExecSQL(QB_Stmt);
