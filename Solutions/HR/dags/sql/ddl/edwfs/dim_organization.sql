CREATE TABLE IF NOT EXISTS {{ params.param_fs_core_dataset_name }}.dim_organization (
org_sid INT64 NOT NULL
, lob_sid INT64
, same_store_sid INT64
, coid STRING
, aso_bso_storage_code STRING
, org_name_parent STRING
, org_name_child STRING NOT NULL
, org_alias_name STRING
, org_coid_alias_name STRING
, alias_table_name STRING
, sort_key_num INT64 NOT NULL
, consolidation_code STRING
, storage_code STRING
, two_pass_calc_code STRING
, formula_text STRING
, member_solve_order_num INT64 NOT NULL
, org_level_uda_name STRING
, org_hier_name STRING
, active_uda_sw INT64
, org_attribute1_sid INT64
, org_attribute1_name STRING
, org_attribute2_sid INT64
, org_attribute2_name STRING
, org_attribute3_sid INT64
, org_attribute3_name STRING
, org_attribute4_sid INT64
, org_attribute4_name STRING
, org_attribute5_sid INT64
, org_attribute5_name STRING
, org_attribute6_sid INT64
, org_attribute6_name STRING
, org_attribute7_sid INT64
, org_attribute7_name STRING
, org_attribute8_sid INT64
, org_attribute8_name STRING
, org_attribute9_sid INT64
, org_attribute9_name STRING
, org_attribute10_sid INT64
, org_attribute10_name STRING
, program_type_sid INT64
, program_type_name STRING
, org_attribute12_sid INT64
, org_attribute12_name STRING
, org_attribute13_sid INT64
, org_attribute13_name STRING
, org_attribute14_sid INT64
, org_attribute14_name STRING
, org_attribute15_sid INT64
, org_attribute15_name STRING
, org_attribute16_sid INT64
, org_attribute16_name STRING
, org_attribute17_sid INT64
, org_attribute17_name STRING
, org_attribute18_sid INT64
, org_attribute18_name STRING
, org_attribute19_sid INT64
, org_attribute19_name STRING
, org_attribute20_sid INT64
, org_attribute20_name STRING
, source_system_code STRING
, dw_last_update_date_time DATETIME
)
 CLUSTER BY Org_SID
;
