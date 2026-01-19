-- Translation time: 2023-05-10T05:47:50.588331Z
-- Translation job ID: 92e92d81-1143-4781-91d8-614cfb5dfe89
-- Source: eim-ops-cs-datamig-dev-0002/sql_conversion/edwcr_migration_source/{{ params.param_cr_views_dataset_name }}/physician_credential.sql
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_cr_views_dataset_name }}.physician_credential AS SELECT
    pc.hcp_dw_id,
    pc.hcp_full_name,
    pc.hcp_last_name,
    pc.hcp_first_name,
    pc.hcp_middle_name,
    pc.hcp_npi,
    pc.upin,
    pc.med_spcl_desc,
    cl.coid AS hospital_coid,
    pc.credentialing_coid,
    pc.market_name,
    pc.city_name,
    pc.state_code,
    pc.credenting_entity_sid,
    pc.credentialing_entity_desc,
    pc.credentialing_type
  FROM
    {{ params.param_cr_base_views_dataset_name }}.clinical_health_care_provider AS cl
    INNER JOIN {{ params.param_cr_base_views_dataset_name }}.physician_credential_subset AS pc ON cl.national_provider_id = pc.hcp_npi
;
