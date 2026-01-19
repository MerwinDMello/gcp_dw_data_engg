-- Translation time: 2024-04-11T17:46:05.729460Z
-- Translation job ID: 292d7f41-1de7-4c69-ae5f-c8272f16f7a3
-- Source: eim-ops-cs-datamig-dev-0002/cr_conversion/6O3Ce4/input/sp_cdm_procedure_detail.sql
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE PROCEDURE `{{ params.param_cr_procs_dataset_name }}`.sp_cdm_procedure_detail()
  BEGIN
    -- #####################################################################################
    -- #                                                                                   																											  #
    -- # Target Table - EDWCR.CDM_Procedure_Detail                                                  																       #
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
    -- #   Loads the CDM_Procedure_Detail table from data available in EDWCDM. This table contains all procedures associated   #
    -- #   with an encounter.        																																						  #
    -- #   																				                           																					  #
    -- #                                                                                   																											  #
    -- #                                                                                  																												  #
    -- #####################################################################################
    -- FIRST STMT
    DECLARE qb_stmt STRING;
    -- Set Queryband
    -- SET QB_Stmt = 'SET QUERY_BAND = ''App=CDM_Sarah_Cannon_ETL;Job=CDM_Procedure_Detail;'' FOR SESSION;';
    -- CALL DBC.SysExecSQL(QB_Stmt);
    --  Delete prior data for Attribute
    TRUNCATE TABLE `{{ params.param_cr_core_dataset_name }}`.cdm_procedure_detail;
    --   Load Attribute Data
    INSERT INTO `{{ params.param_cr_core_dataset_name }}`.cdm_procedure_detail (patient_dw_id, procedure_code, procedure_type_code, procedure_seq_num, service_date, procedure_code_desc, procedure_type_code_desc, coid, company_code, pat_acct_num, procedure_rank_num, facility_physician_num, physician_npi, physician_full_name, physician_last_name, physician_first_name, source_system_code, dw_last_update_date_time)
      SELECT DISTINCT
          pp.patient_dw_id,
          TRIM(pp.procedure_code),
          TRIM(pp.procedure_type_code),
          pp.procedure_seq_num,
          pp.service_date,
          TRIM(refp.procedure_desc) AS procedure_code_desc,
          substr(CASE
             upper(TRIM(pp.procedure_type_code))
            WHEN '0' THEN 'ICD-10'
            WHEN '9' THEN 'ICD-09'
            WHEN '5' THEN 'CPT'
            ELSE ''
          END, 1, 20) AS procedure_type_code_desc,
          pp.coid,
          pp.company_code,
          pp.pat_acct_num,
          pp.procedure_rank_num,
          detail_crnt.facility_physician_num,
          detail_crnt.physician_npi,
          RTRIM(substr(TRIM(detail_crnt.physician_name), 1, 31)) AS physician_full_name,
          RTRIM(substr(TRIM(detail_crnt.physician_last_name), 1, 255)) AS physician_last_name,
          TRIM(detail_crnt.physician_first_name),
          'C' AS source_system_code,
          datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
        FROM
          `{{ params.param_cr_base_views_dataset_name }}`.patient_procedure AS pp
          INNER JOIN (
            SELECT DISTINCT
                cr_patient_date_driver.patient_dw_id
              FROM
                `{{ params.param_cr_base_views_dataset_name }}`.cr_patient_date_driver
          ) AS cr_driver ON pp.patient_dw_id = cr_driver.patient_dw_id
          LEFT OUTER JOIN `{{ params.param_auth_base_views_dataset_name }}`.ref_procedure AS refp ON upper(TRIM(pp.procedure_code)) = upper(TRIM(refp.procedure_code))
           AND upper(TRIM(pp.procedure_type_code)) = upper(TRIM(refp.procedure_type_code))
          LEFT OUTER JOIN `{{ params.param_cr_base_views_dataset_name }}`.physician_detail_crnt AS detail_crnt ON upper(TRIM(detail_crnt.coid)) = upper(TRIM(pp.coid))
           AND upper(TRIM(detail_crnt.company_code)) = upper(TRIM(pp.company_code))
           AND detail_crnt.facility_physician_num = pp.facility_physician_surg_num
        WHERE upper(TRIM(pp.company_code)) = 'H'
        QUALIFY row_number() OVER (PARTITION BY pp.patient_dw_id, upper(pp.procedure_code), upper(pp.procedure_type_code), pp.procedure_seq_num, pp.service_date ORDER BY pp.patient_dw_id) = 1
    ;
  END;
-- LAST STMT
-- Collect Stats
-- CALL DBADMIN_PROCS.collect_stats_table('EDWCR','CDM_Procedure_Detail');
-- Clear Queryband
-- SET QB_Stmt = 'SET QUERY_BAND = NONE FOR SESSION;';
-- CALL DBC.SysExecSQL(QB_Stmt);
