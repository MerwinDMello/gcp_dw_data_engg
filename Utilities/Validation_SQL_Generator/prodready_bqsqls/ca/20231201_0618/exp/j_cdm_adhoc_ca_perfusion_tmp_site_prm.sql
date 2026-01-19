-- Translation time: 2023-12-01T12:21:28.647193Z
-- Translation job ID: 4144eabf-8284-495a-81e3-de22d755910b
-- Source: eim-clin-pdoc-ccda-dev-0001/ca_bulk_conversion_validation/20231201_0618/input/exp/j_cdm_adhoc_ca_perfusion_tmp_site_prm.sql
-- Translated from: Teradata
-- Translated to: BigQuery

SELECT count(*)
FROM
  (SELECT DISTINCT unions.perfusionnumber,
                   unions.tmp_site_id,
                   unions.lowest_core_tmp_amt,
                   unions.full_server_nm
   FROM
     (SELECT cardioaccess_perfusion_stg.perfusionnumber,
             cardioaccess_perfusion_stg.casenumber,
             1 AS tmp_site_id,
             cardioaccess_perfusion_stg.lowctmpbla AS lowest_core_tmp_amt,
             cardioaccess_perfusion_stg.full_server_nm
      FROM {{ params.param_clinical_cdm_stage_dataset_name }}.cardioaccess_perfusion_stg
      UNION DISTINCT SELECT cardioaccess_perfusion_stg.perfusionnumber,
                            cardioaccess_perfusion_stg.casenumber,
                            2 AS tmp_site_id,
                            cardioaccess_perfusion_stg.lowctmpeso AS lowest_core_tmp_amt,
                            cardioaccess_perfusion_stg.full_server_nm
      FROM {{ params.param_clinical_cdm_stage_dataset_name }}.cardioaccess_perfusion_stg
      UNION DISTINCT SELECT cardioaccess_perfusion_stg.perfusionnumber,
                            cardioaccess_perfusion_stg.casenumber,
                            3 AS tmp_site_id,
                            cardioaccess_perfusion_stg.lowctmpnas AS lowest_core_tmp_amt,
                            cardioaccess_perfusion_stg.full_server_nm
      FROM {{ params.param_clinical_cdm_stage_dataset_name }}.cardioaccess_perfusion_stg
      UNION DISTINCT SELECT cardioaccess_perfusion_stg.perfusionnumber,
                            cardioaccess_perfusion_stg.casenumber,
                            4 AS tmp_site_id,
                            cardioaccess_perfusion_stg.lowctmprec AS lowest_core_tmp_amt,
                            cardioaccess_perfusion_stg.full_server_nm
      FROM {{ params.param_clinical_cdm_stage_dataset_name }}.cardioaccess_perfusion_stg
      UNION DISTINCT SELECT cardioaccess_perfusion_stg.perfusionnumber,
                            cardioaccess_perfusion_stg.casenumber,
                            5 AS tmp_site_id,
                            cardioaccess_perfusion_stg.lowctmptym AS lowest_core_tmp_amt,
                            cardioaccess_perfusion_stg.full_server_nm
      FROM {{ params.param_clinical_cdm_stage_dataset_name }}.cardioaccess_perfusion_stg
      UNION DISTINCT SELECT cardioaccess_perfusion_stg.perfusionnumber,
                            cardioaccess_perfusion_stg.casenumber,
                            6 AS tmp_site_id,
                            cardioaccess_perfusion_stg.lowctmpoth AS lowest_core_tmp_amt,
                            cardioaccess_perfusion_stg.full_server_nm
      FROM {{ params.param_clinical_cdm_stage_dataset_name }}.cardioaccess_perfusion_stg) AS unions
   WHERE unions.lowest_core_tmp_amt IS NOT NULL ) AS b