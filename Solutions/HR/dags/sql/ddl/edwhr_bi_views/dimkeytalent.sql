CREATE OR REPLACE VIEW {{ params.param_hr_bi_views_dataset_name }}.dimkeytalent AS SELECT
    ref_bi_key_talent.key_talent_id,
    CASE
      WHEN ref_bi_key_talent.key_talent_id IN(
        12, 13, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47
      ) THEN 'C-Suite'
      WHEN ref_bi_key_talent.key_talent_id IN(
        6, 10, 50, 51, 52, 53, 54, 55, 56, 65, 66
      ) THEN 'ED/SS'
      WHEN ref_bi_key_talent.key_talent_id IN(
        1, 2, 3, 9, 11, 36
      ) THEN 'Feeder Roles'
      WHEN ref_bi_key_talent.key_talent_id IN(
        17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34
      ) THEN 'OSG'
      WHEN ref_bi_key_talent.key_talent_id IN(
        5, 7, 35, 48, 49, 57, 58, 59, 60
      ) THEN 'Other'
      WHEN ref_bi_key_talent.key_talent_id IN(
        8, 14, 15, 16, 61, 62, 63, 64
      ) THEN 'PSG'
      WHEN (ref_bi_key_talent.key_talent_id) = 4 THEN 'Controllers'
      ELSE 'Other'
    END AS key_talent_group,
    CASE
      WHEN upper(ref_bi_key_talent.key_talent_group_text) = 'CEO' THEN 'CEO'
      WHEN upper(ref_bi_key_talent.key_talent_group_text) = 'CNO' THEN 'CNO'
      WHEN upper(ref_bi_key_talent.key_talent_group_text) = 'CMO' THEN 'CMO'
      WHEN upper(ref_bi_key_talent.key_talent_group_text) = 'CFO' THEN 'CFO'
      WHEN upper(ref_bi_key_talent.key_talent_group_text) = 'COO' THEN 'COO'
      WHEN upper(ref_bi_key_talent.key_talent_group_text) = 'DIR EMERGENCY SVCS' THEN 'ED'
      WHEN upper(ref_bi_key_talent.key_talent_group_text) = 'DIR EMERGENCY SVCS FSED' THEN 'ED'
      WHEN upper(ref_bi_key_talent.key_talent_group_text) = 'DIR SURGICAL SVCS' THEN 'SS'
      WHEN upper(ref_bi_key_talent.key_talent_group_text) = 'VP EMERGENCY SVCS' THEN 'ED'
      WHEN upper(ref_bi_key_talent.key_talent_group_text) = 'VP SURGICAL SVCS' THEN 'SS'
      ELSE 'Other'
    END AS key_talent_sub_group,
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
    {{ params.param_hr_base_views_dataset_name }}.ref_bi_key_talent
;
