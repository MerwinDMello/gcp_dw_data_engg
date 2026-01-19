Select 'PBMAR390-40'||','||coalesce(trim(Cast(zeroifnull(Sum(Unbilled_Gross_Bus_Ofc_Amt)) As VARCHAR(20))),'0') ||',' as source_string
From edwpf.Fact_RCOM_AR_Patient_Level
where Date_Sid = Cast(cast((add_months(current_date, -1) (format 'yyyymm')) as char(6)) as integer)
