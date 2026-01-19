-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra_views/cc_facility_iplan.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_parallon_ra_views_dataset_name }}.cc_facility_iplan
   OPTIONS(description='An organization that is financially responsible for paying healthcare claims. The person receiving the medical care is usually a member or subscriber (or related to one) of the organization.')
  AS SELECT
      a.payor_dw_id,
      a.company_code,
      a.coid,
      a.create_date_time,
      a.financial_class_code,
      a.iplan_id,
      a.icd10_conversion_date,
      a.model_covered_population_text,
      a.model_product_class_text,
      a.payer_id,
      a.payer_name,
      a.payer_part_b_ind,
      a.unit_num,
      a.update_date_time,
      a.dw_last_update_date_time,
      a.source_system_code,
      a.ip_provider_num,
      a.op_provider_num
    FROM
      {{ params.param_parallon_ra_base_views_dataset_name }}.cc_facility_iplan AS a
      INNER JOIN {{ params.param_parallon_ra_base_views_dataset_name }}.secref_facility AS b ON rtrim(a.company_code) = rtrim(b.company_code)
       AND rtrim(a.coid) = rtrim(b.co_id)
       AND rtrim(b.user_id) = rtrim(session_user())
  ;
