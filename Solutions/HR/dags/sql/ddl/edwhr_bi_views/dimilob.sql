CREATE OR REPLACE VIEW {{ params.param_hr_bi_views_dataset_name }}.dimilob AS SELECT
    ref_integrated_lob.integrated_lob_id,
    ref_integrated_lob.category_desc AS ilob_category_desc,
    ref_integrated_lob.sub_category_desc AS ilob_sub_category_desc
  FROM
    {{ params.param_hr_base_views_dataset_name }}.ref_integrated_lob
;
