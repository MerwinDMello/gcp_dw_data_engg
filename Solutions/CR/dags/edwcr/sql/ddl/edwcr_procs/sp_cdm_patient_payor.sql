-- Translation time: 2024-04-11T17:46:05.729460Z
-- Translation job ID: 292d7f41-1de7-4c69-ae5f-c8272f16f7a3
-- Source: eim-ops-cs-datamig-dev-0002/cr_conversion/6O3Ce4/input/sp_cdm_patient_payor.sql
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE PROCEDURE `{{ params.param_cr_procs_dataset_name }}`.sp_cdm_patient_payor()
  BEGIN
    -- #####################################################################################
    -- #                                                                                   																											  #
    -- # Target Table - EDWCR.CDM_Patient_Payor                                                  																          #
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
    -- #   Loads the CDM_Patient_Payor table from data available in EDWCDM. Payor information at the Patient level. This table     #
    -- #   contains the primary insurance of the patient.       																		  										  #
    -- #   																			                               																					  #
    -- #                                                                                   																											  #
    -- #                                                                                  																												  #
    -- #####################################################################################
    -- FIRST STMT
    DECLARE qb_stmt STRING;
    -- Set Queryband
    -- SET QB_Stmt = 'SET QUERY_BAND = ''App=CDM_Sarah_Cannon_ETL;Job=CDM_Patient_Payor;'' FOR SESSION;';
    -- CALL DBC.SysExecSQL(QB_Stmt);
    --  Delete prior data for Attribute
    TRUNCATE TABLE `{{ params.param_cr_core_dataset_name }}`.cdm_patient_payor;
    --   Load Attribute Data
    INSERT INTO `{{ params.param_cr_core_dataset_name }}`.cdm_patient_payor (patient_dw_id, coid, company_code, pat_acct_num, payor_dw_id_ins1, payor_name, iplan_id_ins1, plan_desc, major_payor_id_ins1, major_payor_group_desc, financial_class_code, financial_class_desc, source_system_code, dw_last_update_date_time)
      SELECT DISTINCT
          fp.patient_dw_id,
          fp.coid,
          fp.company_code,
          fp.pat_acct_num,
          fp.payor_dw_id_ins1,
          -- Primary Insurance Payor Code
          TRIM(iplan.payor_name),
          fp.iplan_id_ins1,
          -- IPlan code
          TRIM(iplan.plan_desc),
          fp.major_payor_id_ins1,
          -- Patient Major Payor Code
          TRIM(refpayor.major_payor_group_desc),
          fp.financial_class_code,
          -- Type of Insurance Code
          TRIM(refclass.financial_class_desc),
          'C' AS source_system_code,
          datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
        FROM
          `{{ params.param_cr_base_views_dataset_name }}`.fact_patient AS fp
          INNER JOIN (
            SELECT DISTINCT
                cr_patient_date_driver.patient_dw_id
              FROM
                `{{ params.param_cr_base_views_dataset_name }}`.cr_patient_date_driver
          ) AS cr_driver ON fp.patient_dw_id = cr_driver.patient_dw_id
          LEFT OUTER JOIN `{{ params.param_cr_base_views_dataset_name }}`.ref_major_payor_group AS refpayor ON fp.major_payor_id_ins1 = CAST(`{{ params.param_fns_dataset_name }}`.cw_td_normalize_number(refpayor.major_payor_group_code) as FLOAT64)
           AND upper(TRIM(fp.sub_payor_group_code)) = upper(TRIM(refpayor.sub_payor_group_code))
          LEFT OUTER JOIN `{{ params.param_auth_base_views_dataset_name }}`.ref_company_financial_class AS refclass ON upper(TRIM(fp.company_code)) = upper(TRIM(refclass.company_code))
           AND fp.financial_class_code = refclass.financial_class_code
          LEFT OUTER JOIN `{{ params.param_cr_base_views_dataset_name }}`.facility_iplan AS iplan ON upper(TRIM(fp.coid)) = upper(TRIM(iplan.coid))
           AND upper(TRIM(fp.company_code)) = upper(TRIM(iplan.company_code))
           AND fp.iplan_id_ins1 = iplan.iplan_id
           AND fp.payor_dw_id_ins1 = iplan.payor_dw_id
        WHERE upper(TRIM(fp.company_code)) = 'H'
    ;
  END;
-- LAST STMT
-- Collect Stats
-- CALL DBADMIN_PROCS.collect_stats_table('EDWCR','CDM_Patient_Payor');
-- Clear Queryband
-- SET QB_Stmt = 'SET QUERY_BAND = NONE FOR SESSION;';
-- CALL DBC.SysExecSQL(QB_Stmt);
