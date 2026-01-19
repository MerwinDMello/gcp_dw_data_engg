-- Translation time: 2024-04-11T17:46:05.729460Z
-- Translation job ID: 292d7f41-1de7-4c69-ae5f-c8272f16f7a3
-- Source: eim-ops-cs-datamig-dev-0002/cr_conversion/6O3Ce4/input/sp_cdm_ed_visit.sql
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE PROCEDURE `{{ params.param_cr_procs_dataset_name }}`.sp_cdm_encounter()

BEGIN
    -- #####################################################################################
    -- #                                                                                   																											  
    -- # Target Table - EDWCR.CDM_Encounter                                                  																			  
    -- #                                                                                   																											  
    -- # CHANGE CONTROL:                                                                   																						      
    -- #                                                                                   																											  
    -- # DATE          Developer     Change Comment                                        																					  
    -- # 03/18/2019    J.Huertas       Initial Version                                     																						  
    -- #                                                                                   																											  
    -- #                                                                                   																											  
    -- # Created as part of Sarah Cannon Project                                          																						  
    -- #                                                                                   																											  
    -- #                     															  																												  
    -- #                                                                                   																											  
    -- #   Loads the CDM_Encounter table from data available in EDWCDM. CDM_Encounter contains all the patient encounters.   
    -- #   																 																															  
    -- #                                 																					  																			  
    -- #                                                                                   																											  
    -- #                                                                                  																												  
    -- #####################################################################################

-- FIRST STMT
DECLARE qb_stmt STRING;
-- Set Queryband
-- SET QB_Stmt = 'SET QUERY_BAND = ''App=CDM_Sarah_Cannon_ETL;Job=CDM_Encounter;'' FOR SESSION;';
-- CALL DBC.SysExecSQL(QB_Stmt);

--  Delete prior data for Attribute
TRUNCATE TABLE `{{ params.param_cr_core_dataset_name }}`.cdm_encounter;

-- Load Attribute Data
INSERT INTO `{{ params.param_cr_core_dataset_name }}`.cdm_encounter (
  patient_dw_id,
  pat_acct_num,
  coid,
  company_code,
  patient_sk,  -- NUMERIC(29)
  facility_sk,
  medical_record_num,
  patient_market_urn,
  arrival_mode_sk,
  arrival_mode_code,
  arrival_mode_desc,
  admit_source_sk,
  admit_source_code,
  admit_source_desc,
  admit_type_code,
  visit_type_sk,
  visit_type_code,
  visit_type_desc,
  special_program_sk,
  special_program_code,
  special_program_desc,
  discharge_status_sk,
  discharge_status_code,
  discharge_status_desc,
  encounter_date_time,
  admission_date_time,
  accident_date_time,
  discharge_date_time,
  reason_for_visit_text,
  actual_los_cnt,
  signature_date,
  readmission_ind,
  source_system_text,
  source_system_code,
  dw_last_update_date_time
)
SELECT DISTINCT
  fe.patient_dw_id,
  fe.pat_acct_num,
  fe.coid,
  fe.company_code,
  CAST(fe.patient_sk AS NUMERIC),  -- BIGNUMERIC
  fe.facility_sk,
  TRIM(fe.medical_record_num),
  TRIM(fe.patient_market_urn),
  NULL AS arrival_mode_sk,
  RTRIM(SUBSTR(TRIM(fe.arrival_mode_code), 1, 150)) AS arrival_mode_code,
  TRIM(fe.arrival_mode_desc),
  NULL AS admit_source_sk,
  RTRIM(SUBSTR(TRIM(fe.admit_source_code), 1, 150)) AS admit_source_code,
  TRIM(fe.admit_source_desc),
  TRIM(fe.admit_type_code),
  NULL AS visit_type_sk,
  RTRIM(SUBSTR(TRIM(fe.visit_type_code), 1, 150)) AS visit_type_code,
  CAST(NULL AS STRING) AS visit_type_desc,
  NULL AS special_program_sk,
  TRIM(fe.special_program_code),
  TRIM(fe.special_program_desc),
  NULL AS discharge_status_sk,
  RTRIM(SUBSTR(TRIM(fe.discharge_status_code), 1, 150)) AS discharge_status_code,
  TRIM(fe.discharge_status_desc),
  fe.encounter_date_time,
  fe.admission_date_time,
  fe.accident_date_time,
  fe.discharge_date_time,
  TRIM(fe.reason_for_visit_txt) AS reason_for_visit_text,
  fe.actl_los_cnt AS actual_los_cnt,
  fe.signature_date,
  TRIM(fe.readmission_ind),
  TRIM(fe.source_system_txt) AS source_system_text,
  'C' AS source_system_code,
  DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) AS dw_last_update_date_time
FROM `hca-hin-prod-cur-ops.auth_base_views.fact_encounter` AS fe
JOIN (
  SELECT DISTINCT cr_patient_date_driver.patient_dw_id
  FROM `{{ params.param_cr_base_views_dataset_name }}`.cr_patient_date_driver
) AS a ON a.patient_dw_id = fe.patient_dw_id
;

END