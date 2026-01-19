-- Translation time: 2023-04-25T18:33:50.275152Z
-- Translation job ID: 8b921d45-66b0-461e-880c-7106e48c9005
-- Source: eim-cs-da-gmek-5764-dev/HRG/Bteq_Source_Files/SARANYADEVI/missing_views/EDWHR_Base_Views/dim_organization.sql
-- Translated from: Teradata
-- Translated to: BigQuery

/***************************************************************************************
   B A S E   V I E W
****************************************************************************************/
CREATE OR REPLACE VIEW {{ params.param_hr_base_views_dataset_name }}.dim_organization AS SELECT
    dim_organization.org_sid,
    dim_organization.lob_sid,
    dim_organization.same_store_sid,
    dim_organization.coid,
    dim_organization.aso_bso_storage_code,
    dim_organization.org_name_parent,
    dim_organization.org_name_child,
    dim_organization.org_alias_name,
    dim_organization.org_coid_alias_name,
    dim_organization.alias_table_name,
    dim_organization.sort_key_num,
    dim_organization.consolidation_code,
    dim_organization.storage_code,
    dim_organization.two_pass_calc_code,
    dim_organization.formula_text,
    dim_organization.member_solve_order_num,
    dim_organization.org_level_uda_name,
    dim_organization.org_hier_name,
    dim_organization.active_uda_sw,
    dim_organization.org_attribute1_sid,
    dim_organization.org_attribute1_name,
    dim_organization.org_attribute2_sid,
    dim_organization.org_attribute2_name,
    dim_organization.org_attribute3_sid,
    dim_organization.org_attribute3_name,
    dim_organization.org_attribute4_sid,
    dim_organization.org_attribute4_name,
    dim_organization.org_attribute5_sid,
    dim_organization.org_attribute5_name,
    dim_organization.org_attribute6_sid,
    dim_organization.org_attribute6_name,
    dim_organization.org_attribute7_sid,
    dim_organization.org_attribute7_name,
    dim_organization.org_attribute8_sid,
    dim_organization.org_attribute8_name,
    dim_organization.org_attribute9_sid,
    dim_organization.org_attribute9_name,
    dim_organization.org_attribute10_sid,
    dim_organization.org_attribute10_name,
    dim_organization.program_type_sid,
    dim_organization.program_type_name,
    dim_organization.org_attribute12_sid,
    dim_organization.org_attribute12_name,
    dim_organization.org_attribute13_sid,
    dim_organization.org_attribute13_name,
    dim_organization.org_attribute14_sid,
    dim_organization.org_attribute14_name,
    dim_organization.org_attribute15_sid,
    dim_organization.org_attribute15_name,
    dim_organization.org_attribute16_sid,
    dim_organization.org_attribute16_name,
    dim_organization.org_attribute17_sid,
    dim_organization.org_attribute17_name,
    dim_organization.org_attribute18_sid,
    dim_organization.org_attribute18_name,
    dim_organization.org_attribute19_sid,
    dim_organization.org_attribute19_name,
    dim_organization.org_attribute20_sid,
    dim_organization.org_attribute20_name,
    dim_organization.source_system_code,
    dim_organization.dw_last_update_date_time
  FROM
    {{ params.param_fs_base_views_dataset_name }}.dim_organization
;
