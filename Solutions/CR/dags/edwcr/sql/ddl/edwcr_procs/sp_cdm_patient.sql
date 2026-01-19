-- Translation time: 2024-04-11T17:46:05.729460Z
-- Translation job ID: 292d7f41-1de7-4c69-ae5f-c8272f16f7a3
-- Source: eim-ops-cs-datamig-dev-0002/cr_conversion/6O3Ce4/input/sp_cdm_patient.sql
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE PROCEDURE `{{ params.param_cr_procs_dataset_name }}`.sp_cdm_patient()
  BEGIN
    -- #####################################################################################
    -- #                                                                                   																											  #
    -- # Target Table - EDWCR.CDM_Patient                                                  																					  #
    -- #                                                                                   																											  #
    -- # CHANGE CONTROL:                                                                   																						      #
    -- #                                                                                   																											  #
    -- # DATE          Developer     Change Comment                                        																					  #
    -- # 03/18/2019    J.Huertas       Initial Version                                     																						  #
    -- #                                                                                   																											  #
    -- #                                                                                   																											  #
    -- # Created as part of Sarah Cannon Project                                          																						  #
    -- #                                                                                   																											  #
    -- #                     															  																												  #
    -- #                                                                                   																											  #
    -- #   Loads the CDM_Patient tables from data available in EDWCDM. This table contains the patient information (name, 		  #
    -- #   address, telephone number, gender, etc.)   																		  													  #
    -- #   																					                       																					  #
    -- #                                                                                   																											  #
    -- #                                                                                  																												  #
    -- #####################################################################################
    -- FIRST STMT
    DECLARE qb_stmt STRING;
    -- Set Queryband
    -- SET QB_Stmt = 'SET QUERY_BAND = ''App=CDM_Sarah_Cannon_ETL;Job=CDM_Patient;'' FOR SESSION;';
    -- CALL DBC.SysExecSQL(QB_Stmt);
    --  Delete prior data for Attribute
    TRUNCATE TABLE `{{ params.param_cr_core_dataset_name }}`.cdm_patient;
    --   Load Attribute Data
    INSERT INTO `{{ params.param_cr_core_dataset_name }}`.cdm_patient (role_plyr_sk, empi_text, patient_race_code_sk, patient_race_desc, address_line_1_text, address_line_2_text, city_name, state_code, zip_code, home_phone_num, business_phone_num, mobile_phone_num, birth_date_time, death_date_time, first_name, middle_name, last_name, gender_code, patient_email_text, vital_status_id, source_system_code, dw_last_update_date_time)
      SELECT DISTINCT
          pd.role_plyr_sk AS role_plyr_sk,
          TRIM(pd.umpi_txt) AS empi_text,
          pd.race_cd AS patient_race_code_sk,
          TRIM(race.race_desc) AS patient_race_desc,
          TRIM(pd.addr_line1_desc) AS address_line_1_text,
          TRIM(pd.addr_line2_desc) AS address_line_2_text,
          TRIM(pd.city_nm) AS city_name,
          TRIM(pd.st_prv_cd) AS state_code,
          TRIM(pd.pstl_cd) AS zip_code,
          TRIM(pd.hme_ph_nbr) AS home_phone_num,
          TRIM(pd.bsns_ph_nbr) AS business_phone_num,
          TRIM(pd.mobl_ph_nbr) AS mobile_phone_num,
          pd.brth_ts AS birth_date_time,
          pd.dth_ts AS death_date_time,
          RTRIM(substr(TRIM(pd.frst_nm), 1, 50)) AS first_name,
          RTRIM(substr(TRIM(pd.mid_nm), 1, 50)) AS middle_name,
          RTRIM(substr(TRIM(pd.lst_nm), 1, 50)) AS last_name,
          TRIM(pd.sex_ref_cd) AS gender_code,
          TRIM(pd.hme_eml) AS patient_email_text,
          TRIM(pd.liv_sts_ref_cd) AS vital_status_id,
          'C' AS source_system_code,
          datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
        FROM
          `{{ params.param_cr_base_views_dataset_name }}`.ptnt_dtl AS pd
          INNER JOIN `{{ params.param_cr_base_views_dataset_name }}`.dim_race AS race 
          ON UPPER(TRIM(COALESCE(race.race_code,'Unknown'))) = UPPER(TRIM(COALESCE(pd.race_cd,'Unknown')))
          AND UPPER(TRIM(COALESCE(race.race_desc,'Unknown'))) = UPPER(TRIM(COALESCE(pd.race_desc,'Unknown')))
          AND pd.vld_to_ts = DATETIME '9999-12-31 00:00:00'
    ;
  END;
-- LAST STMT
-- Collect Stats
-- CALL DBADMIN_PROCS.collect_stats_table('EDWCR','CDM_Patient');
-- Clear Queryband
-- SET QB_Stmt = 'SET QUERY_BAND = NONE FOR SESSION;';
-- CALL DBC.SysExecSQL(QB_Stmt);
