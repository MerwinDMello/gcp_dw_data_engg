CREATE OR REPLACE VIEW {{ params.param_clm_views_dataset_name }}.fact_claim
AS SELECT
		a.claim_id,
		a.vendor_cid,
		a.unit_num,
		a.bill_type_code,
		a.bill_provider_sid,
		a.pay_to_provider_sid,
		a.total_charge_amt,
		a.payor_seq_ind,
		a.iplan_id,
		a.bill_dt,
		a.patient_acct_num,
		a.med_rec_num,
		a.fed_tax_num,
		a.stmt_cover_from_dt,
		a.stmt_cover_to_dt,
		a.admission_dt,
		a.admission_hr,
		a.admission_type_ind,
		a.admission_source_cd,
		a.discharge_hr,
		a.discharge_status_cd,
		a.accident_st,
		a.npi,
		a.admit_diag_code,
		a.drg,
		a.claim_desc,
		a.file_link_path_txt,
		a.claim_file_name,
		a.dw_last_update_date_time,
		a.source_system_code,
		a.prefix_pat_acct_num,
		a.pas_coid,
		a.financial_class,
		a.patient_type,
		a.edi_837_type,
		a.numeric_unit_num,
		a.numeric_patient_acct_num,
		a.service_code,
		a.taxonomy_code,
		a.destination_method,
		a.operationalgroup
  FROM
    {{ params.param_clm_base_views_dataset_name }}.fact_claim AS a
    INNER JOIN {{ params.param_clm_base_views_dataset_name }}.facility_dimension AS fd ON rtrim(fd.unit_num, ' ') = rtrim(a.unit_num, ' ')
    INNER JOIN {{ params.param_clm_base_views_dataset_name }}.secref_facility AS b ON rtrim(fd.coid, ' ') = rtrim(b.co_id, ' ')
	AND rtrim(fd.company_code, ' ') = rtrim(b.company_code, ' ')
    AND rtrim(b.user_id, ' ') = rtrim(session_user(), ' ')
;
