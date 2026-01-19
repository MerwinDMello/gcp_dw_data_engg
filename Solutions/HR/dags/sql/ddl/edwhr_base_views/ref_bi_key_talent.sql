create or replace view `{{ params.param_hr_base_views_dataset_name }}.ref_bi_key_talent`
AS SELECT
  ref_bi_key_talent.key_talent_id,
  ref_bi_key_talent.job_code,
  ref_bi_key_talent.facility_level_code,
  ref_bi_key_talent.key_talent_group_text,
  ref_bi_key_talent.job_title_text,
  ref_bi_key_talent.lob_code,
  ref_bi_key_talent.job_code_desc,
  ref_bi_key_talent.process_level_code,
  ref_bi_key_talent.match_level_num,
  ref_bi_key_talent.match_level_desc,
  ref_bi_key_talent.source_system_code,
  ref_bi_key_talent.dw_last_update_date_time
FROM
 {{ params.param_hr_core_dataset_name }}.ref_bi_key_talent;