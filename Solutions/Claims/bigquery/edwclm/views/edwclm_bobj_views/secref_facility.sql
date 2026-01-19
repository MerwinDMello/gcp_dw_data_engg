CREATE OR REPLACE VIEW edwclm_bobj_views.secref_facility
  AS
    SELECT a.company_code, a.user_id, a.co_id
    FROM {{ params.param_curated_project_id }}.edwclm_base_views.secref_facility AS a
