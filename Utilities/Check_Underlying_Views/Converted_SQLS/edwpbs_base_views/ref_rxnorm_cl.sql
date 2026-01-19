-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/ref_rxnorm_cl.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.ref_rxnorm_cl AS SELECT
    ref_rxnorm.rxnorm_id,
    ref_rxnorm.rxnorm_desc,
    ref_rxnorm.term_type_code,
    ref_rxnorm.strength_amt_text,
    ref_rxnorm.human_drug_code,
    ref_rxnorm.vet_drug_code,
    ref_rxnorm.quantity_text,
    ref_rxnorm.quantity_amt_text,
    ref_rxnorm.quantity_unit_text,
    ref_rxnorm.ingredient_name,
    ref_rxnorm.num_of_ingredient_num,
    ref_rxnorm.alternate_name,
    ref_rxnorm.prescribable_name,
    ref_rxnorm.prescribable_ind,
    ref_rxnorm.term_form_desc,
    ref_rxnorm.brand_name_cardinality_text,
    ref_rxnorm.status_ind,
    ref_rxnorm.boss_from_code,
    ref_rxnorm.boss_active_ingredient_text,
    ref_rxnorm.boss_active_moiety_text,
    ref_rxnorm.boss_strength_denom_value_num,
    ref_rxnorm.boss_strength_denom_unit_text,
    ref_rxnorm.boss_strength_num_unit_text,
    ref_rxnorm.boss_strength_value_num,
    ref_rxnorm.concept_status_desc,
    ref_rxnorm.suppress_code,
    ref_rxnorm.expressed_flag_code,
    ref_rxnorm.replacement_term_code_num,
    ref_rxnorm.qualitative_distinction_text,
    ref_rxnorm.term_type_seq_num,
    ref_rxnorm.preferred_sw,
    ref_rxnorm.iso_language_code,
    ref_rxnorm.effective_date_time,
    ref_rxnorm.obsolete_date_time,
    ref_rxnorm.modification_date_time,
    ref_rxnorm.last_published_date_time,
    ref_rxnorm.source_system_code,
    ref_rxnorm.dw_last_update_date_time
  FROM
    `hca-hin-dev-cur-parallon`.edwcl_base_views.ref_rxnorm
;
