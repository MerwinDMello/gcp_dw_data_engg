-- Translation time: 2023-04-25T18:33:50.275152Z
-- Translation job ID: 8b921d45-66b0-461e-880c-7106e48c9005
-- Source: eim-cs-da-gmek-5764-dev/HRG/Bteq_Source_Files/SARANYADEVI/missing_views/EDWFS_Base_Views/ref_fa_measure.sql
-- Translated from: Teradata
-- Translated to: BigQuery

/***************************************************************************************
   B A S E   V I E W
****************************************************************************************/
CREATE OR REPLACE VIEW {{ params.param_fs_base_views_dataset_name }}.ref_fa_measure AS SELECT
    ref_fa_measure.fa_measure_code,
    ref_fa_measure.fa_measure_desc,
    ref_fa_measure.source_system_code,
    ref_fa_measure.dw_last_update_date_time
  FROM
    {{ params.param_fs_core_dataset_name }}.ref_fa_measure
;
