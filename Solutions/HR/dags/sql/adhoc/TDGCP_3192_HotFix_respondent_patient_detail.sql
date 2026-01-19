update hca-hin-prod-cur-hr.edwhr.respondent_patient_detail as tgt
set patient_dw_id = src.patient_dw_id
from (
select coid,patient_account_num, patient_dw_id from 
hca-hin-prod-cur-hr.auth_base_views.hl7_clinical_keys
group by coid,patient_account_num, patient_dw_id
) as src
where tgt.coid = src.coid
and tgt.pat_acct_num = src.patient_account_num
and tgt.pat_acct_num is not null
and tgt.pat_acct_num <> 0 
and tgt.patient_dw_id is null;