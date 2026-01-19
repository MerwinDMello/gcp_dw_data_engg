-- Translation time: 2024-04-11T17:46:05.729460Z
-- Translation job ID: 292d7f41-1de7-4c69-ae5f-c8272f16f7a3
-- Source: eim-ops-cs-datamig-dev-0002/cr_conversion/6O3Ce4/input/sp_cdm_call_procedures.sql
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE PROCEDURE `{{ params.param_cr_procs_dataset_name }}`.sp_cdm_call_procedures()
  BEGIN
    -- #####################################################################################
    -- #                                                                                   																											  #
    -- # Target Table - EDWCR.CDM_Encounter                                            																					  #
    -- #                                                                                   																											  #
    -- # CHANGE CONTROL:                                                                   																						      #
    -- #                                                                                   																											  #
    -- # DATE          Developer     Change Comment                                        																					  #
    -- # 03/20/2019    J.Huertas       Initial Version                                     																						  #
    -- #                                                                                   																											  #
    -- #                                                                                   																											  #
    -- # Created as part of Sarah Cannon Project to execute procedures                                         													  #
    -- #                                                                                   																											  #
    -- #                     															  																												  #
    -- #                                                                                   																											  #
    -- #   Stored procedures calls all Sarah Cannon stored procedures to loads EDWCR tables from data available in EDWCDM      #
    -- #   so that Sarah Cannon can get more information concerning their patients with cancer and get a fuller    			                  #
    -- #   understanding of all the care they've received.        																		                                      #
    -- #                                  																					  																			  #
    -- #                                                                                   																											  #
    -- #                                                                                  																												  #
    -- #####################################################################################
    -- FIRST STMT
    DECLARE qb_stmt STRING;
    CALL  `{{ params.param_cr_procs_dataset_name }}`.sp_cdm_encounter();
    CALL  `{{ params.param_cr_procs_dataset_name }}`.sp_cdm_patient();
    CALL  `{{ params.param_cr_procs_dataset_name }}`.sp_cdm_procedure_detail();
    CALL  `{{ params.param_cr_procs_dataset_name }}`.sp_cdm_patient_diagnosis();
    CALL  `{{ params.param_cr_procs_dataset_name }}`.sp_cdm_care_team();
    CALL  `{{ params.param_cr_procs_dataset_name }}`.sp_cdm_patient_payor();
    CALL  `{{ params.param_cr_procs_dataset_name }}`.sp_cdm_ed_visit();
    CALL  `{{ params.param_cr_procs_dataset_name }}`.sp_cdm_ed_visit_order_detail();
    CALL  `{{ params.param_cr_procs_dataset_name }}`.sp_cdm_surgery();
    CALL  `{{ params.param_cr_procs_dataset_name }}`.sp_cdm_patient_medication();
    CALL  `{{ params.param_cr_procs_dataset_name }}`.sp_cdm_imaging_result_detail();
    CALL  `{{ params.param_cr_procs_dataset_name }}`.sp_cdm_patient_history();
    CALL  `{{ params.param_cr_procs_dataset_name }}`.sp_cdm_biopsy();
    CALL  `{{ params.param_cr_procs_dataset_name }}`.sp_cdm_lab_result_detail();
  END;
-- Set Queryband
-- SET QB_Stmt = 'SET QUERY_BAND = ''App=CDM_Sarah_Cannon;Job=CDM_Call_Procedures;'' FOR SESSION;';
-- CALL DBC.SysExecSQL(QB_Stmt);
-- LAST STMT
-- Clear Queryband
-- SET QB_Stmt = 'SET QUERY_BAND = NONE FOR SESSION;';
-- CALL DBC.SysExecSQL(QB_Stmt);
