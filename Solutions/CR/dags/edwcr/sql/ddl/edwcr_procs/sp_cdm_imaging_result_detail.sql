-- Translation time: 2024-04-11T17:46:05.729460Z
-- Translation job ID: 292d7f41-1de7-4c69-ae5f-c8272f16f7a3
-- Source: eim-ops-cs-datamig-dev-0002/cr_conversion/6O3Ce4/input/sp_cdm_imaging_result_detail.sql
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE PROCEDURE `{{ params.param_cr_procs_dataset_name }}`.sp_cdm_imaging_result_detail()
  BEGIN
    -- #####################################################################################
    -- #                                                                                   																											  #
    -- # Target Table - EDWCR.CDM_Imaging_Result_Detail                                                  															  #
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
    -- #   Loads the CDM_Imaging_Result_Detail table from data available in EDWCDM.  Contains all the imaging results for the      #
    -- #   patient.																											      																	  #
    -- #  																				                            																					  #
    -- #                                                                                   																											  #
    -- #                                                                                  																												  #
    -- #####################################################################################
    -- FIRST STMT
    DECLARE qb_stmt STRING;
    -- Set Queryband
    -- SET QB_Stmt = 'SET QUERY_BAND = ''App=CDM_Sarah_Cannon_ETL;Job=CDM_Imaging_Result_Detail;'' FOR SESSION;';
    -- CALL DBC.SysExecSQL(QB_Stmt);
    --  Delete prior data for Attribute
    TRUNCATE TABLE `{{ params.param_cr_core_dataset_name }}`.cdm_imaging_result_detail;
    --   Load Attribute Data
    INSERT INTO `{{ params.param_cr_core_dataset_name }}`.cdm_imaging_result_detail (clinical_finding_sk, patient_dw_id, coid, company_code, image_occurence_ts, source_system_original_code, imaging_type_code, procedure_mnemonic_cs, rad_exam_name, source_system_code, dw_last_update_date_time)
      SELECT DISTINCT
          id.clnc_fnd_sk AS clinical_finding_sk,
          id.patient_dw_id,
          id.coid,
          id.company_code,
          id.imag_occr_ts AS image_occurence_ts,
          TRIM(id.src_sys_orgnl_cd) AS source_system_original_code,
          TRIM(`{{ params.param_fns_dataset_name }}`.cw_td_strtok(id.src_sys_orgnl_cd, '_', 1)) AS imaging_type_code,
          -- Cast to CHAR(10) to match the data type on the table
          TRIM(substr(id.src_sys_orgnl_cd, strpos(id.src_sys_orgnl_cd, '_') + 1)) AS procedure_mnemonic_cs,
          -- Cast to CHAR(10) to match the data type on the table
          TRIM(coalesce(rre.rad_exam_name, id.imag_rslt_desc, cmn.cd_desc)) AS rad_exam_name,
          'C' AS source_system_code,
          datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
        FROM
          `{{ params.param_cr_base_views_dataset_name }}`.imag_rslt_dtl AS id
          INNER JOIN (
            SELECT DISTINCT
                cr_patient_date_driver.patient_dw_id
              FROM
                `{{ params.param_cr_base_views_dataset_name }}`.cr_patient_date_driver
          ) AS a ON a.patient_dw_id = id.patient_dw_id
          LEFT OUTER JOIN `{{ params.param_cr_base_views_dataset_name }}`.ref_rad_exam AS rre ON upper(TRIM(substr(id.src_sys_orgnl_cd, strpos(id.src_sys_orgnl_cd, '_') + 1))) = upper(TRIM(rre.rad_exam_mnemonic))
           AND upper(TRIM(`{{ params.param_fns_dataset_name }}`.cw_td_strtok(id.src_sys_orgnl_cd, '_', 1))) = upper(TRIM(rre.rad_exam_type_code))
           AND upper(TRIM(id.coid)) = upper(TRIM(rre.coid))
           AND upper(TRIM(id.company_code)) = upper(TRIM(rre.company_code))
          LEFT OUTER JOIN `{{ params.param_cr_base_views_dataset_name }}`.cd_anch AS anch ON id.imag_rslt_cd = CAST(anch.cd_anch_sk AS STRING)
           AND anch.vld_to_ts = DATETIME '9999-12-31 00:00:00'
          LEFT OUTER JOIN `{{ params.param_cr_base_views_dataset_name }}`.cmn_cd AS cmn ON cmn.cd_sk = anch.cd_sk
           AND cmn.vld_to_ts = DATETIME '9999-12-31 00:00:00'
        WHERE DATE(id.vld_to_ts) = DATE '9999-12-31'
    ;
  END;
-- LAST STMT
-- Collect Stats
-- CALL DBADMIN_PROCS.collect_stats_table('EDWCR','CDM_Imaging_Result_Detail');
-- Clear Queryband
-- SET QB_Stmt = 'SET QUERY_BAND = NONE FOR SESSION;';
-- CALL DBC.SysExecSQL(QB_Stmt);
