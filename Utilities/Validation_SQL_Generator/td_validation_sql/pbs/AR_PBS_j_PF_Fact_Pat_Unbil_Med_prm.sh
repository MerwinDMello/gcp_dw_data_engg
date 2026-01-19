
export AC_EXP_TOLERANCE_PERCENT=0
export AC_EXP_TOLERANCE_AMT=0
export JOBNAME='PBMAR900-50'

export AC_EXP_SQL_STATEMENT="Select  'PBMAR900-50'||','||trim(cast(zeroifnull(sum(du.unbilled_charge_amt)) as varchar(20))) ||','as Source_string
from 
Edwpf_base_Views.discharged_unbilled du

Join Edwpf_base_Views.Facility_Dimension FD
on FD.Company_Code = du.Company_Code
and FD.COID = du.COID

WHERE 
cast((du.effective_date (format 'yyyymm')) as char(6)) = cast((add_months(current_date, -1) (format 'yyyymm')) as char(6)) AND
du.unbilled_responsibility_ind  = 'M'
AND (du.Company_Code = 'H' OR substr(Trim(FD.Company_Code_Operations),1,1) = 'Y')
AND (du.Account_Process_Ind = 'S')"

export AC_ACT_SQL_STATEMENT="Select 'PBMAR900-50'||','|| trim(cast((zeroifnull(Sum(Unbilled_Gross_Med_Rec_Amt))) as varchar(30)) )||',' as Source_String
From Edwpbs_Base_Views.Fact_RCOM_AR_Patient_Level
where Date_Sid = Cast(cast((add_months(current_date, -1) (format 'yyyymm')) as char(6)) as integer)
"
