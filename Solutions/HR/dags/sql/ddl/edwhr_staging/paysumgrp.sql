create table if not exists `{{ params.param_hr_stage_dataset_name }}.paysumgrp`
(
  company INT64 NOT NULL,
  pay_sum_grp STRING NOT NULL,
  description STRING NOT NULL,
  check_desc STRING NOT NULL,
  tip_pay_type INT64 NOT NULL,
  elig_ot_pay STRING NOT NULL,
  elig_ot_hrs STRING NOT NULL,
  elig_prem_pay STRING NOT NULL,
  elig_prem_cred STRING NOT NULL,
  supp_tax_code STRING NOT NULL,
  change_flag STRING NOT NULL,
  country_code STRING NOT NULL,
  payment_type STRING NOT NULL,
  income_type STRING NOT NULL,
  remun_code STRING NOT NULL,
  tax_form_type INT64 NOT NULL,
  rptable_1099r INT64 NOT NULL,
  psgset2_ss_sw STRING NOT NULL
)
