-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra_base_views/ref_837_bill_type.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_parallon_ra_base_views_dataset_name }}.ref_837_bill_type AS SELECT
    ref_837_bill_type.bill_type_code,
    ref_837_bill_type.bill_type_desc,
    ref_837_bill_type.dw_last_update_date_time,
    ref_837_bill_type.source_system_code
  FROM
    {{ params.param_auth_base_views_dataset_name }}.ref_837_bill_type
;
