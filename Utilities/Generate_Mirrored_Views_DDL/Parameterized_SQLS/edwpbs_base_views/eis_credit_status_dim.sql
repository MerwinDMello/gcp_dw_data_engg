CREATE OR REPLACE VIEW {{ params.param_pbs_base_views_dataset_name }}.eis_credit_status_dim
  AS 
select
    dim_credit_status.credit_status_sid,
    dim_credit_status.credit_status_name_child as credit_status_member,
    dim_credit_status.credit_status_child_alias_name as credit_status_alias
  from
    {{ params.param_pbs_base_views_dataset_name }}.dim_credit_status
;