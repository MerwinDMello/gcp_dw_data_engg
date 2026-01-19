-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra_views/ref_cc_eapg_visit_type_code.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_parallon_ra_views_dataset_name }}.ref_cc_eapg_visit_type_code
   OPTIONS(description='Reference table for descriptions related to enhanced all payer grouping visit type codes.  This table will contain static values.')
  AS SELECT
      a.eapg_visit_type_code,
      a.eapg_visit_type_code_desc,
      a.source_system_code,
      a.dw_last_update_date_time
    FROM
      {{ params.param_parallon_ra_base_views_dataset_name }}.ref_cc_eapg_visit_type_code AS a
  ;
