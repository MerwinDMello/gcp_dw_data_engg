-- Translation time: 2023-05-10T05:47:50.588331Z
-- Translation job ID: 92e92d81-1143-4781-91d8-614cfb5dfe89
-- Source: eim-ops-cs-datamig-dev-0002/sql_conversion/edwcr_migration_source/{{ params.param_cr_views_dataset_name }}/rxnorm_drug_cd.sql
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_cr_views_dataset_name }}.rxnorm_drug_cd AS SELECT
    rxnorm_drug_cd.rxnorm_drug_cd_sk,
    rxnorm_drug_cd.vld_fr_ts,
    rxnorm_drug_cd.rxn_cui,
    rxnorm_drug_cd.rxn_aui,
    rxnorm_drug_cd.rxn_nm,
    rxnorm_drug_cd.ndc_cd,
    rxnorm_drug_cd.dose_form,
    rxnorm_drug_cd.dose_form_rxn_cui,
    rxnorm_drug_cd.qntfd_form,
    rxnorm_drug_cd.qntfd_form_rxn_cui,
    rxnorm_drug_cd.clr_cd,
    rxnorm_drug_cd.clr_desc,
    rxnorm_drug_cd.ctng,
    rxnorm_drug_cd.shape,
    rxnorm_drug_cd.shape_desc,
    rxnorm_drug_cd.sz,
    rxnorm_drug_cd.va_class_cd,
    rxnorm_drug_cd.va_class_desc,
    rxnorm_drug_cd.va_dspns_unt,
    rxnorm_drug_cd.va_gnrc_nm,
    rxnorm_drug_cd.smntc_type,
    rxnorm_drug_cd.smntc_type_desc,
    rxnorm_drug_cd.trm_type,
    rxnorm_drug_cd.trm_type_desc,
    rxnorm_drug_cd.strg,
    rxnorm_drug_cd.lblr,
    rxnorm_drug_cd.rt_of_admn,
    rxnorm_drug_cd.nrmlzd_strg,
    rxnorm_drug_cd.csa_cd,
    rxnorm_drug_cd.csa_desc,
    rxnorm_drug_cd.hccpc_j_cd,
    rxnorm_drug_cd.hccpc_j_cd_desc,
    rxnorm_drug_cd.ddd_unt,
    rxnorm_drug_cd.def_daily_dose,
    rxnorm_drug_cd.vld_to_ts,
    rxnorm_drug_cd.prgncy_rsk_factor_cd,
    rxnorm_drug_cd.prgncy_rsk_factor_desc,
    rxnorm_drug_cd.unii_cd,
    rxnorm_drug_cd.unii_desc,
    rxnorm_drug_cd.otc_sts,
    rxnorm_drug_cd.cd_sys_version_sk,
    rxnorm_drug_cd.version_vld_fr_ts,
    rxnorm_drug_cd.crt_run_id,
    rxnorm_drug_cd.lst_updt_run_id,
    rxnorm_drug_cd.dw_insrt_ts
  FROM
    {{ params.param_cr_base_views_dataset_name }}.rxnorm_drug_cd
;
