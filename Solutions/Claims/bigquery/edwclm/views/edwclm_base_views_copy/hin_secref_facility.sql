CREATE OR REPLACE VIEW {{ params.param_clm_mirrored_base_views_dataset_name }}.hin_secref_facility
AS SELECT
	company_code,
	user_id,
	co_id
  FROM
    {{ params.param_clm_core_dataset_name }}.hin_secref_facility
;
