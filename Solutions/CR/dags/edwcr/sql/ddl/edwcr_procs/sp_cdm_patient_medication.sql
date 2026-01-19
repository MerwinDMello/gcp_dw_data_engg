-- Translation time: 2024-04-11T17:46:05.729460Z
-- Translation job ID: 292d7f41-1de7-4c69-ae5f-c8272f16f7a3
-- Source: eim-ops-cs-datamig-dev-0002/cr_conversion/6O3Ce4/input/sp_cdm_patient_medication.sql
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE PROCEDURE `{{ params.param_cr_procs_dataset_name }}`.sp_cdm_patient_medication()
BEGIN
    -- #####################################################################################
    -- #                                                                                   																											  #
    -- # Target Table - EDWCR.CDM_Patient_Medication                                                  																  #
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
    -- #   Loads the CDM_Patient_Medication table from data available in EDWCDM. This table contains the information of the   	  #
    -- #   medication administered to the patient        																		  													  #
    -- #   																			                               																					  #
    -- #                                                                                   																											  #
    -- #                                                                                  																												  #
    -- #####################################################################################
    -- FIRST STMT
  DECLARE qb_stmt STRING;
    -- Set Queryband
    -- SET QB_Stmt = 'SET QUERY_BAND = ''App=CDM_Sarah_Cannon_ETL;Job=CDM_Patient_Medication;'' FOR SESSION;';
    -- CALL DBC.SysExecSQL(QB_Stmt);

-- Delete prior data for Attribute
TRUNCATE TABLE `{{ params.param_cr_core_dataset_name }}`.cdm_patient_medication;

-- Load Attribute Data
INSERT INTO `{{ params.param_cr_core_dataset_name }}`.cdm_patient_medication (
  medication_admn_sk,
  patient_dw_id, coid,
  company_code,
  medication_desc,
  occurence_ts,
  drug_dose_amt_text,
  drug_dose_measurement_text,
  administrative_frequency_text,
  administered_unit_cnt,
  route_code_sk,
  route_code_desc,
  ordering_physician_name,
  physician_npi,
  medication_num_text,
  source_system_original_code,
  clinical_pharmacy_trade_name,
  source_system_code,
  dw_last_update_date_time
)
SELECT DISTINCT
  m.mdctn_admn_sk AS medication_admn_sk,
  m.patient_dw_id AS patient_dw_id,
  m.coid AS coid,
  m.company_code AS company_code,
  TRIM(m.rxnorm_drug_desc) AS medication_desc,
  m.occr_ts AS occurence_ts,
  TRIM(m.admn_tot_dsge_txt) AS drug_dose_amt_text,
  TRIM(m.admn_dsge_unt_of_msr_cd) AS drug_dose_measurement_text,
  TRIM(m.admn_freq_txt) AS administrative_frequency_text,
  m.admn_unt_cnt AS administered_unit_cnt,
  TRIM(m.rt_cd) AS route_code_sk,
  TRIM(m.rt_desc) AS route_code_desc,
  TRIM(pi.fl_nm) AS ordering_physician_name,
  ROUND(CAST(`{{ params.param_fns_dataset_name }}`.cw_td_normalize_number(pri.id_txt) as NUMERIC), 0, 'ROUND_HALF_EVEN') AS physician_npi,
  TRIM(m.rx_num_txt) AS medication_num_text,
  TRIM(m.src_sys_orgnl_cd) AS source_system_original_code,
  TRIM(rcp.clinical_phrm_trade_name) AS clinical_pharmacy_trade_name,
  'C' AS source_system_code,
  datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
  FROM
    `{{ params.param_auth_base_views_dataset_name }}`.mdctn_admn_dtl AS m
    INNER JOIN (
      SELECT DISTINCT cr_patient_date_driver.patient_dw_id FROM {{ params.param_cr_base_views_dataset_name }}.cr_patient_date_driver
    ) AS a ON a.patient_dw_id = m.patient_dw_id

    LEFT OUTER JOIN `{{ params.param_auth_base_views_dataset_name }}`.phrm_ordr_to_mdctn_admn AS poma ON m.mdctn_admn_sk = poma.mdctn_admn_sk
      AND m.patient_dw_id = poma.patient_dw_id
      AND poma.vld_to_ts = DATETIME '9999-12-31 00:00:00'
    LEFT OUTER JOIN `{{ params.param_auth_base_views_dataset_name }}`.phrm_ordr_dtl AS pod ON pod.phrm_ordr_sk = poma.phrm_ordr_sk
      AND pod.patient_dw_id = poma.patient_dw_id
      AND pod.vld_to_ts = DATETIME '9999-12-31 00:00:00'
    LEFT OUTER JOIN (
      SELECT
        por.phrm_ordr_sk,
        por.patient_dw_id,
        pd.role_plyr_sk,
        por.vld_fr_ts,
        pd.prctnr_role_cd,
        pd.fl_nm
      FROM `{{ params.param_auth_base_views_dataset_name }}`.phrm_ordr_to_role AS por
      LEFT OUTER JOIN `{{ params.param_auth_base_views_dataset_name }}`.prctnr_dtl AS pd ON pd.role_plyr_sk = por.role_plyr_sk AND pd.vld_to_ts = DATETIME '9999-12-31 00:00:00'
      WHERE por.vld_to_ts = DATETIME '9999-12-31 00:00:00' AND upper(TRIM(pd.prctnr_role_cd)) = '1HCALIP'
    ) AS pi ON pi.phrm_ordr_sk = pod.phrm_ordr_sk
      AND pi.patient_dw_id = pod.patient_dw_id
    LEFT OUTER JOIN
   `{{ params.param_auth_base_views_dataset_name }}`.prctnr_role_idfn AS pri ON pi.role_plyr_sk = pri.role_plyr_sk
      AND pri.vld_to_ts = DATETIME '9999-12-31 00:00:00'
      AND upper(TRIM(pri.registn_type_ref_cd)) = 'NPI'
    LEFT OUTER JOIN `{{ params.param_auth_base_views_dataset_name }}`.ref_clinical_pharmaceutical AS rcp ON TRIM(m.src_sys_orgnl_cd) = TRIM(rcp.clinical_phrm_mnem_cs)
      AND upper(TRIM(m.coid)) = upper(TRIM(rcp.coid))
      AND upper(TRIM(m.company_code)) = upper(TRIM(rcp.company_code))
  WHERE m.vld_to_ts = DATETIME '9999-12-31 00:00:00' AND upper(TRIM(pod.ordr_sts_ref_cd)) <> 'CA'
  QUALIFY row_number() OVER (PARTITION BY medication_admn_sk ORDER BY upper(pod.msg_ctrl_id_txt) DESC, pi.vld_fr_ts DESC) = 1
;
END;
-- LAST STMT
-- Collect Stats
-- CALL DBADMIN_PROCS.collect_stats_table('EDWCR','CDM_Patient_Medication');
-- Clear Queryband
-- SET QB_Stmt = 'SET QUERY_BAND = NONE FOR SESSION;';
-- CALL DBC.SysExecSQL(QB_Stmt);
