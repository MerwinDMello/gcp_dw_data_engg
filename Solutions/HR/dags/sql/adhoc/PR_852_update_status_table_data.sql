update hca-hin-prod-cur-hr.edwhr.status as tgt 
set tgt.active_dw_ind = src.active_dw_ind,
tgt.valid_to_date = datetime_trunc(datetime(src.valid_to_date),second)
from hca-hin-dev-cur-hr.edwhr.status as src
where tgt.status_sid = src.status_sid
and date(tgt.valid_from_date) = date(src.valid_from_date)
and tgt.hr_company_sid = src.hr_company_sid
and tgt.lawson_company_num = 300
and upper(trim(tgt.source_system_code)) = 'A'
and upper(trim(tgt.active_dw_ind)) <> upper(trim(src.active_dw_ind));
