-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra_base_views/secref_facility_mv.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_parallon_ra_base_views_dataset_name }}.secref_facility_mv AS SELECT
    secref_facility_mv.company_code,
    secref_facility_mv.user_id,
    secref_facility_mv.co_id
  FROM
    {{ params.param_parallon_ra_core_dataset_name }}.secref_facility_mv
;
