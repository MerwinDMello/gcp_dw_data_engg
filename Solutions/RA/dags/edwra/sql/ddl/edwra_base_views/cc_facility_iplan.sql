-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra_base_views/cc_facility_iplan.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_parallon_ra_base_views_dataset_name }}.cc_facility_iplan
   OPTIONS(description='An organization that is financially responsible for paying healthcare claims. The person receiving the medical care is usually a member or subscriber (or related to one) of the organization.')
  AS SELECT
      cc_facility_iplan.payor_dw_id,
      cc_facility_iplan.company_code,
      cc_facility_iplan.coid,
      cc_facility_iplan.create_date_time,
      cc_facility_iplan.financial_class_code,
      cc_facility_iplan.iplan_id,
      cc_facility_iplan.icd10_conversion_date,
      cc_facility_iplan.model_covered_population_text,
      cc_facility_iplan.model_product_class_text,
      cc_facility_iplan.payer_id,
      cc_facility_iplan.payer_name,
      cc_facility_iplan.payer_part_b_ind,
      cc_facility_iplan.unit_num,
      cc_facility_iplan.update_date_time,
      cc_facility_iplan.dw_last_update_date_time,
      cc_facility_iplan.source_system_code,
      cc_facility_iplan.ip_provider_num,
      cc_facility_iplan.op_provider_num
    FROM
      {{ params.param_parallon_ra_core_dataset_name }}.cc_facility_iplan
  ;
