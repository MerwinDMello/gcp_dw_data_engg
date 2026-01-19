-- Translation time: 2024-04-11T17:46:05.729460Z
-- Translation job ID: 292d7f41-1de7-4c69-ae5f-c8272f16f7a3
-- Source: eim-ops-cs-datamig-dev-0002/cr_conversion/6O3Ce4/input/sp_cdm_biopsy.sql
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE PROCEDURE `{{ params.param_cr_procs_dataset_name }}`.sp_cdm_biopsy()

BEGIN
-- #####################################################################################
-- #                                                                                   																											  #
-- # Target Table - EDWCR.CDM_Biopsy                                                  																			          #
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
-- #   Loads the CDM_Biopsy table from data available in EDWCDM (PRCDR_DTL). Contains all the patient biopsies.  			  #
-- #   The field PRCDR_TXT has been filtered to only capture those records that contain the word Biopsy.      							  #
-- #   																		                                 																					  #
-- #                                                                                   																											  #
-- #                                                                                  																												  #
-- #####################################################################################

-- FIRST STMT
DECLARE qb_stmt STRING;

-- Set Queryband
-- SET QB_Stmt = 'SET QUERY_BAND = ''App=CDM_Sarah_Cannon_ETL;Job=CDM_Biopsy;'' FOR SESSION;';
-- CALL DBC.SysExecSQL(QB_Stmt);

--  Delete prior data for Attribute
TRUNCATE TABLE `{{ params.param_cr_core_dataset_name }}`.cdm_biopsy;
    
--   Load Attribute Data
INSERT INTO `{{ params.param_cr_core_dataset_name }}`.cdm_biopsy (
  procedure_sk,
  procedure_text,
  patient_dw_id,
  coid,
  company_code,
  biopsy_ts,
  biopsy_performing_physician_name,
  physician_specialty_name,
  role_plyr_sk,
  physician_npi,
  priority_sequence_num,
  anesthesia_code_sk,
  anesthesia_code_desc,
  encounter_sk,
  source_system_code,
  dw_last_update_date_time
)
SELECT DISTINCT
  prcdr.prcdr_sk AS procedure_sk,
  TRIM(prcdr.prcdr_txt) AS procedure_text,
  prcdr.patient_dw_id,
  prcdr.coid,
  prcdr.company_code AS company_code,
  prcdr.prcdr_ts AS biopsy_ts,
  TRIM(pd.fl_nm) AS biopsy_performing_physician_name,
  TRIM(sp.spcly_nm) AS physician_specialty_name,
  pr.role_plyr_sk AS role_plyr_sk,
  CAST(`{{ params.param_fns_dataset_name }}`.cw_td_normalize_number(pri.id_txt) as INT64) AS physician_npi,
  prcdr.prty_seq AS priority_sequence_num,
  prcdr.ansth_cd_sk AS anesthesia_code_sk,
  TRIM(cmn.cd_desc) AS anesthesia_code_desc,
  prcdr.encnt_sk AS encounter_sk,
  'C' AS source_system_code,
  datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
FROM `{{ params.param_auth_base_views_dataset_name }}`.prcdr_dtl AS prcdr
JOIN (
  SELECT DISTINCT cr_patient_date_driver.patient_dw_id
  FROM `{{ params.param_cr_base_views_dataset_name }}`.cr_patient_date_driver
) AS a ON a.patient_dw_id = prcdr.patient_dw_id

LEFT OUTER JOIN `{{ params.param_cr_base_views_dataset_name }}`.cd_anch AS anch ON anch.cd_anch_sk = prcdr.ansth_cd_sk AND 
anch.vld_to_ts = DATETIME '9999-12-31 00:00:00'
  
LEFT OUTER JOIN `{{ params.param_cr_base_views_dataset_name }}`.cmn_cd AS cmn ON cmn.cd_sk = anch.cd_sk AND 
cmn.vld_to_ts = DATETIME '9999-12-31 00:00:00'

LEFT OUTER JOIN `{{ params.param_cr_base_views_dataset_name }}`.prcdr_to_role AS pr ON pr.prcdr_sk = prcdr.prcdr_sk AND 
UPPER(TRIM(pr.rl_type_ref_cd)) = 'PRF' AND pr.vld_to_ts = DATETIME '9999-12-31 00:00:00'

--  Relationship Type Reference Code (to identify the type of relationship between the two entities in this association)
LEFT OUTER JOIN `{{ params.param_auth_base_views_dataset_name }}`.prctnr_dtl AS pd ON pd.role_plyr_sk = pr.role_plyr_sk
AND pd.vld_to_ts = DATETIME '9999-12-31 00:00:00'

LEFT OUTER JOIN `{{ params.param_cr_base_views_dataset_name }}`.prctnr_spcly AS sp ON sp.role_plyr_sk = pd.role_plyr_sk
AND sp.vld_to_ts = DATETIME '9999-12-31 00:00:00'

LEFT OUTER JOIN `{{ params.param_auth_base_views_dataset_name }}`.prctnr_role_idfn AS pri ON pri.role_plyr_sk = pd.role_plyr_sk
AND UPPER(pri.registn_type_ref_cd) LIKE '%NPI%'
AND pri.vld_to_ts = DATETIME '9999-12-31 00:00:00'

WHERE UPPER(prcdr.prcdr_txt) LIKE '%BIOPSY%' AND prcdr.vld_to_ts = DATETIME '9999-12-31 00:00:00'
QUALIFY row_number() OVER (PARTITION BY procedure_sk ORDER BY UPPER(TRIM(physician_specialty_name))) = 1
;

END;
-- 
--  to filter only the procedures that contain the word biopsy in the text
-- LAST STMT
-- Collect Stats
-- CALL DBADMIN_PROCS.collect_stats_table('EDWCR','CDM_Biopsy');
-- Clear Queryband
-- SET QB_Stmt = 'SET QUERY_BAND = NONE FOR SESSION;';
-- CALL DBC.SysExecSQL(QB_Stmt);