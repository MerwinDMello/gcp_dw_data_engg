-- Translation time: 2024-04-11T17:46:05.729460Z
-- Translation job ID: 292d7f41-1de7-4c69-ae5f-c8272f16f7a3
-- Source: eim-ops-cs-datamig-dev-0002/cr_conversion/6O3Ce4/input/sp_cdm_ed_visit_order_detail.sql
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE PROCEDURE `{{ params.param_cr_procs_dataset_name }}`.sp_cdm_ed_visit_order_detail()
  BEGIN
    -- #####################################################################################
    -- #                                                                                   																											  #
    -- # Target Table - EDWCR.CDM_ED_Visit_Order_Detail                                                  															  #
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
    -- #   Loads CDM_ED_Visit_Order_Detail table from data available in EDWCDM. Contains emergency department data at 		  #
    -- #   the order level for a patient. Includes treatments, procedures, and meditcations ordered for a patient.       							  #
    -- #   																					                       																					  #
    -- #                                                                                   																											  #
    -- #                                                                                  																												  #
    -- #####################################################################################
    -- FIRST STMT
    DECLARE qb_stmt STRING;
    -- Set Queryband
    -- SET QB_Stmt = 'SET QUERY_BAND = ''App=CDM_Sarah_Cannon_ETL;Job=CDM_ED_Visit_Order_Detail;'' FOR SESSION;';
    -- CALL DBC.SysExecSQL(QB_Stmt);
    --  Delete prior data for Attribute
    TRUNCATE TABLE `{{ params.param_cr_core_dataset_name }}`.cdm_ed_visit_order_detail;
    --   Load Attribute Data
    INSERT INTO `{{ params.param_cr_core_dataset_name }}`.cdm_ed_visit_order_detail (patient_dw_id, order_urn, coid, company_code, pat_acct_num, order_date_time, order_proc_num, order_proc_mnemonic_cs, order_proc_name, source_system_code, dw_last_update_date_time)
      SELECT DISTINCT
          factorder.patient_dw_id,
          TRIM(factorder.order_urn),
          factorder.coid,
          factorder.company_code,
          factorder.pat_acct_num,
          factorder.order_date_time,
          TRIM(factorder.order_proc_num),
          TRIM(refproc.order_proc_mnemonic_cs) AS order_proc_mnemonic_cs,
          -- Cast to CHAR(10) to match the data type on the table
          RTRIM(substr(TRIM(refproc.order_proc_name), 1, 30)) AS order_proc_name,
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
          INNER JOIN `{{ params.param_cr_base_views_dataset_name }}`.fact_ed_order AS factorder ON factorder.patient_dw_id = pd.patient_dw_id
          INNER JOIN `{{ params.param_cr_base_views_dataset_name }}`.ref_order_procedure AS refproc ON upper(TRIM(factorder.coid)) = upper(TRIM(refproc.coid))
           AND upper(TRIM(factorder.company_code)) = upper(TRIM(refproc.company_code))
           AND upper(TRIM(factorder.order_proc_num)) = upper(TRIM(refproc.order_proc_num))
           AND upper(TRIM(factorder.clinical_proc_category_code)) = upper(TRIM(refproc.order_proc_category_code))
           AND TRIM(factorder.clinical_proc_mnem_cs) = TRIM(refproc.order_proc_mnemonic_cs)
        WHERE factorder.order_date_time <= pd.depart_date_time
    ;
  END;
-- LAST STMT
-- Collect Stats
-- CALL DBADMIN_PROCS.collect_stats_table('EDWCR','CDM_ED_Visit_Order_Detail');
-- Clear Queryband
-- SET QB_Stmt = 'SET QUERY_BAND = NONE FOR SESSION;';
-- CALL DBC.SysExecSQL(QB_Stmt);
