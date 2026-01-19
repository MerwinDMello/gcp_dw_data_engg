Locking table edwpbs.Fact_RCOM_PARS_Discrepancy for access
Select 'PBMPC290-30' || ',' ||'0' || ',' ||
trim(coalesce(Cast(Sum(0) as varchar(20)),0))  || ',' || 
trim(coalesce(Cast(Sum(0) as varchar(20)),0))  || ',' || 
trim(coalesce(Cast(Sum(0) as varchar(20)),0))  || ',' || 
trim(coalesce(Cast(Sum(0) as varchar(20)),0))  || ',' || 
trim(coalesce(Cast(sum(Var_Gross_Rbmt_End_Amt) as varchar(20)),0))  || ',' ||
trim(coalesce(Cast(sum(Var_Cont_Alw_End_Amt) as varchar(20)),0))  || ',' ||
trim(coalesce(Cast(sum(Var_Payment_End_Amt)  as varchar(20)),0)) || ',' ||
trim(coalesce(Cast(sum(Var_Charge_End_Amt) as varchar(20)),0))  || ','  AS SOURCE_STRING
From EDWPBS.Fact_RCOM_PARS_Discrepancy
where Date_Sid = Cast(cast((add_months(current_date, -1) (format 'yyyymm')) as char(6)) as integer)
and Discrepancy_Resolved_Date  = '0001-01-01'
and Patient_Sid <> 999999999999
AND coid not in (select coid from edwpbs.parallon_client_detail where Company_code='CHP')