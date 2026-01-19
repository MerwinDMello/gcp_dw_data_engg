Select 'PBMAR390-50'||','|| trim(cast((zeroifnull(Sum(Unbilled_Gross_Med_Rec_Amt))) as varchar(30)) )||',' as Source_String
From Edwpf_Base_Views.Fact_RCOM_AR_Patient_Level
where Date_Sid = Cast(cast((add_months(current_date, -1) (format 'yyyymm')) as char(6)) as integer)
