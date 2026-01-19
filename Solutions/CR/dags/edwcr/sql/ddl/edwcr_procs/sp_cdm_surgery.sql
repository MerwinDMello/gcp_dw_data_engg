-- Translation time: 2024-04-11T17:46:05.729460Z
-- Translation job ID: 292d7f41-1de7-4c69-ae5f-c8272f16f7a3
-- Source: eim-ops-cs-datamig-dev-0002/cr_conversion/6O3Ce4/input/sp_cdm_surgery.sql
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE PROCEDURE `{{ params.param_cr_procs_dataset_name }}`.sp_cdm_surgery()
  BEGIN
    -- #####################################################################################
    -- #                                                                                   																											  #
    -- # Target Table - EDWCR.CDM_Surgery                                                  																                  #
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
    -- #   Loads the CDM_Surgery table from data available in EDWCDM. This table contains information regarding each patients    #
    -- #   surgery.																								         																		  #
    -- #   																			                              																					  #
    -- #                                                                                   																											  #
    -- #                                                                                  																												  #
    -- #####################################################################################
    -- FIRST STMT
    DECLARE qb_stmt STRING;
    -- Set Queryband
    -- SET QB_Stmt = 'SET QUERY_BAND = ''App=CDM_Sarah_Cannon_ETL;Job=CDM_Surgery;'' FOR SESSION;';
    -- CALL DBC.SysExecSQL(QB_Stmt);
    --  Delete prior data for Attribute
    TRUNCATE TABLE `{{ params.param_cr_core_dataset_name }}`.cdm_surgery;
    --   Load Attribute Data
    INSERT INTO `{{ params.param_cr_core_dataset_name }}`.cdm_surgery (patient_dw_id, surgical_case_seq, activity_date, coid, company_code, pat_acct_num, primary_surgeon_dw_id, surgeon_npi, physician_name, surgeon_start_date_id, surgeon_start_time, surgeon_end_date_id, surgeon_end_time, procedure_desc, source_system_code, dw_last_update_date_time)
      SELECT DISTINCT
          f.patient_dw_id,
          f.surg_case_seq AS surgical_case_seq,
          f.activity_date,
          f.coid,
          f.company_code,
          f.pat_acct_num,
          f.primary_surgeon_dw_id,
          d.national_provider_id AS surgeon_npi,
          TRIM(d.physician_name),
          f.surgeon_start_dt_id AS surgeon_start_date_id,
          f.surgeon_start_time,
          f.surgeon_end_dt_id AS surgeon_end_date_id,
          f.surgeon_end_time,
          TRIM(f.procedure_desc),
          'C' AS source_system_code,
          datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
        FROM
          `{{ params.param_cr_base_views_dataset_name }}`.fact_surgical_case_detail AS f
          INNER JOIN (
            SELECT DISTINCT
                cr_patient_date_driver.patient_dw_id
              FROM
                `{{ params.param_cr_base_views_dataset_name }}`.cr_patient_date_driver
          ) AS s ON s.patient_dw_id = f.patient_dw_id
          LEFT OUTER JOIN (
            SELECT
                chcp.consolidated_provider_dw_id,
                chcp.company_code,
                chcp.coid,
                chcp.hcp_mnemonic_cs,
                CASE
                  WHEN chcp.provider_taxonomy_code IS NOT NULL THEN chcp.provider_taxonomy_code
                  ELSE 'UNKNOWN'
                END AS nppes_taxonomy_code,
                coalesce(chcp.national_provider_id, b.national_provider_id) AS national_provider_id,
                CASE
                  WHEN upper(TRIM(chcp.hcp_title)) = 'MD'
                   AND TRIM(substr(chcp.hcp_mnemonic_cs, 1, 1)) IN(
                    '1', '2', '3', '4', '5', '6', '7', '8', '9', '0'
                  ) THEN concat(chcp.hcp_full_name, ' ', chcp.hcp_title)
                  ELSE CASE
                    WHEN chcp.hcp_full_name IS NOT NULL THEN chcp.hcp_full_name
                    ELSE CASE
                      WHEN chcp.hcp_full_name IS NULL THEN b.hcp_full_name
                      ELSE 'UNKNOWN'
                    END
                  END
                END AS physician_name,
                'No_Physician_Group' AS physician_group_name,
                chcp.dw_last_update_date_time
              FROM
                `{{ params.param_cr_base_views_dataset_name }}`.consolidated_health_care_provider AS chcp
                LEFT OUTER JOIN (
                  SELECT DISTINCT
                      a.hcp_mnemonic_cs,
                      a.source_system_key,
                      a.coid,
                      chcp_0.hcp_full_name,
                      chcp_0.national_provider_id
                    FROM
                      (
                        SELECT DISTINCT
                            i.role_plyr_sk AS source_system_key,
                            i.id_txt AS hcp_mnemonic_cs,
                            m.coid
                          FROM
                            `{{ params.param_auth_base_views_dataset_name }}`.prctnr_role_idfn AS i
                            INNER JOIN `{{ params.param_cr_base_views_dataset_name }}`.srgcl_team_mbr AS m ON i.role_plyr_sk = m.prctnr_sk
                             AND m.vld_to_ts = DATETIME '9999-12-31 00:00:00'
                          WHERE upper(TRIM(i.registn_type_ref_cd)) = 'MNEMONIC'
                           AND i.vld_to_ts = DATETIME '9999-12-31 00:00:00'
                      ) AS a
                      LEFT OUTER JOIN --  AND i.ROLE_PLYR_SK = 12223819
                      `{{ params.param_cr_base_views_dataset_name }}`.clinical_health_care_provider AS chcp_0 ON upper(TRIM(a.hcp_mnemonic_cs)) = upper(TRIM(chcp_0.hcp_mnemonic_cs))
                       AND upper(TRIM(a.coid)) = upper(TRIM(chcp_0.coid))
                       AND upper(TRIM(chcp_0.hcp_mnemonic_cs)) <> 'UNDEFINED'
                  UNION DISTINCT
                  SELECT DISTINCT
                      a.hcp_mnemonic_cs,
                      a.source_system_key,
                      '00000' AS coid,
                      chcp_0.hcp_full_name,
                      chcp_0.national_provider_id
                    FROM
                      (
                        SELECT DISTINCT
                            i.role_plyr_sk AS source_system_key,
                            i.id_txt AS hcp_mnemonic_cs,
                            m.coid
                          FROM
                            `{{ params.param_auth_base_views_dataset_name }}`.prctnr_role_idfn AS i
                            INNER JOIN `{{ params.param_cr_base_views_dataset_name }}`.srgcl_team_mbr AS m ON i.role_plyr_sk = m.prctnr_sk
                             AND m.vld_to_ts = DATETIME '9999-12-31 00:00:00'
                          WHERE upper(TRIM(i.registn_type_ref_cd)) = 'MNEMONIC'
                           AND i.vld_to_ts = DATETIME '9999-12-31 00:00:00'
                      ) AS a
                      INNER JOIN --  AND i.ROLE_PLYR_SK = 12223819
                      `{{ params.param_cr_base_views_dataset_name }}`.clinical_health_care_provider AS chcp_0 ON upper(TRIM(a.hcp_mnemonic_cs)) = upper(TRIM(chcp_0.hcp_mnemonic_cs))
                       AND upper(TRIM(a.coid)) = upper(TRIM(chcp_0.coid))
                       AND upper(TRIM(chcp_0.hcp_mnemonic_cs)) <> 'UNDEFINED'
                ) AS b ON chcp.source_system_key = b.source_system_key
                 AND upper(TRIM(chcp.coid)) = upper(TRIM(b.coid))
              WHERE upper(TRIM(chcp.source_system_code)) = 'C'
              QUALIFY row_number() OVER (PARTITION BY chcp.consolidated_provider_dw_id ORDER BY chcp.consolidated_provider_dw_id) = 1
          ) AS d ON f.primary_surgeon_dw_id = d.consolidated_provider_dw_id
    ;
  END;
-- LAST STMT
-- Collect Stats
-- CALL DBADMIN_PROCS.collect_stats_table('EDWCR','CDM_Surgery');
-- Clear Queryband
-- SET QB_Stmt = 'SET QUERY_BAND = NONE FOR SESSION;';
-- CALL DBC.SysExecSQL(QB_Stmt);
