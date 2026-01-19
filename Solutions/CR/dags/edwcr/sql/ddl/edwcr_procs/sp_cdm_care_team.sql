-- Translation time: 2024-04-11T17:46:05.729460Z
-- Translation job ID: 292d7f41-1de7-4c69-ae5f-c8272f16f7a3
-- Source: eim-ops-cs-datamig-dev-0002/cr_conversion/6O3Ce4/input/sp_cdm_care_team.sql
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE PROCEDURE `{{ params.param_cr_procs_dataset_name }}`.sp_cdm_care_team()
  BEGIN
    -- #####################################################################################
    -- #                                                                                   																											  #
    -- # Target Table - EDWCR.CDM_Care_Team                                                  																              #
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
    -- #   Loads the CDM_Care_Team table from data available in EDWCDM. Contains information about the providers that  			  #
    -- #   were involved in a patient's encounter.       																		                                                      #
    -- #   																		                                																					  #
    -- #                                                                                   																											  #
    -- #                                                                                  																												  #
    -- #####################################################################################
    -- FIRST STMT
    DECLARE qb_stmt STRING;
    -- Set Queryband
    -- SET QB_Stmt = 'SET QUERY_BAND = ''App=CDM_Sarah_Cannon_ETL;Job=CDM_Care_Team;'' FOR SESSION;';
    -- CALL DBC.SysExecSQL(QB_Stmt);
    --  Delete prior data for Attribute
    TRUNCATE TABLE `{{ params.param_cr_core_dataset_name }}`.cdm_care_team;
    --   Load Attribute Data
    INSERT INTO `{{ params.param_cr_core_dataset_name }}`.cdm_care_team (patient_dw_id, role_plyr_sk, practitioner_role_type, practitioner_role_type_desc, company_code, coid, practitioner_first_name, practitioner_middle_name, practitioner_last_name, practitioner_full_name, practitioner_specialty_name, practitioner_mnemonic_cs, practitioner_npi, source_system_code, dw_last_update_date_time)
      SELECT DISTINCT
          encnt.patient_dw_id AS patient_dw_id,
          encnt.role_plyr_sk AS role_plyr_sk,
          RTRIM(substr(TRIM(encnt.rl_type_ref_cd), 1, 30)) AS practitioner_role_type,
          substr(CASE
             upper(TRIM(encnt.rl_type_ref_cd))
            WHEN 'ADM' THEN 'Admitting'
            WHEN 'CON' THEN 'Consulting'
            WHEN 'ED_PRACTITIONER' THEN 'Emergency Department Practitioner'
            WHEN 'NURSE' THEN 'Nurse'
            WHEN 'DIS' THEN 'Discharging'
            WHEN 'ATND' THEN 'Attending'
            WHEN 'REF' THEN 'Referring'
            ELSE ''
          END, 1, 80) AS practitioner_role_type_desc,
          encnt.company_code,
          encnt.coid AS coid,
          TRIM(dlt.frst_nm) AS practitioner_first_name,
          TRIM(dlt.mid_nm) AS practitioner_middle_name,
          TRIM(dlt.lst_nm) AS practitioner_last_name,
          TRIM(dlt.fl_nm) AS practitioner_full_name,
          TRIM(spc.spcly_nm) AS practitioner_specialty_name,
          -- ,MNEM.ID_TXT AS Practitioner_Mnemonic_CS
          TRIM(CASE
            WHEN upper(mnem.id_txt) LIKE 'UNDEFINED%' THEN CAST(NULL as STRING)
            ELSE mnem.id_txt
          END) AS practitioner_mnemonic_cs,
          ROUND(CAST(`{{ params.param_fns_dataset_name }}`.cw_td_normalize_number(npi.id_txt) as NUMERIC), 0, 'ROUND_HALF_EVEN') AS practitioner_npi,
          'C' AS source_system_code,
          datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
        FROM
          `{{ params.param_cr_base_views_dataset_name }}`.encnt_to_role AS encnt
          INNER JOIN (
            SELECT DISTINCT
                cr_patient_date_driver.patient_dw_id
              FROM
                `{{ params.param_cr_base_views_dataset_name }}`.cr_patient_date_driver
          ) AS driver ON driver.patient_dw_id = encnt.patient_dw_id
           AND encnt.vld_to_ts = DATETIME '9999-12-31 00:00:00'
          INNER JOIN `{{ params.param_auth_base_views_dataset_name }}`.prctnr_dtl AS dlt ON encnt.role_plyr_sk = dlt.role_plyr_sk
           AND upper(trim(encnt.role_plyr_type_ref_cd)) = 'PRCTNR_DTL'
           AND dlt.vld_to_ts = DATETIME '9999-12-31 00:00:00'
          LEFT OUTER JOIN `{{ params.param_cr_base_views_dataset_name }}`.prctnr_spcly AS spc ON encnt.role_plyr_sk = spc.role_plyr_sk
           AND spc.vld_to_ts = DATETIME '9999-12-31 00:00:00'
          LEFT OUTER JOIN -- AND ENCNT.Patient_DW_Id = SPC.ROLE_PLYR_DW_ID
          `{{ params.param_auth_base_views_dataset_name }}`.prctnr_role_idfn AS mnem ON encnt.role_plyr_sk = mnem.role_plyr_sk
           AND upper(TRIM(mnem.registn_type_ref_cd)) = 'MNEMONIC'
           AND mnem.vld_to_ts = DATETIME '9999-12-31 00:00:00'
          LEFT OUTER JOIN `{{ params.param_auth_base_views_dataset_name }}`.prctnr_role_idfn AS npi ON encnt.role_plyr_sk = npi.role_plyr_sk
           AND upper(TRIM(npi.registn_type_ref_cd)) = 'NPI'
           AND npi.vld_to_ts = DATETIME '9999-12-31 00:00:00'
        WHERE upper(TRIM(npi.id_txt)) NOT IN(
          'Z456789012'
        )
        QUALIFY row_number() OVER (PARTITION BY patient_dw_id, role_plyr_sk, upper(encnt.rl_type_ref_cd) ORDER BY upper(npi.id_txt)) = 1
    ;
  END;
-- LAST STMT
-- Collect Stats
-- CALL DBADMIN_PROCS.collect_stats_table('EDWCR','CDM_Care_Team');
-- Clear Queryband
-- SET QB_Stmt = 'SET QUERY_BAND = NONE FOR SESSION;';
-- CALL DBC.SysExecSQL(QB_Stmt);
