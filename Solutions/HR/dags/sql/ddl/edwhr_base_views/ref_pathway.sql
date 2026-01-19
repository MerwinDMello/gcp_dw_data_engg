CREATE OR REPLACE VIEW {{ params.param_hr_base_views_dataset_name }}.ref_pathway AS SELECT
    ref_pathway.pathway_id,
    ref_pathway.pathway_num,
    ref_pathway.subpathway_code,
    ref_pathway.pathway_name,
    ref_pathway.source_system_code,
    ref_pathway.dw_last_update_date_time
  FROM
    {{ params.param_hr_core_dataset_name }}.ref_pathway
;
