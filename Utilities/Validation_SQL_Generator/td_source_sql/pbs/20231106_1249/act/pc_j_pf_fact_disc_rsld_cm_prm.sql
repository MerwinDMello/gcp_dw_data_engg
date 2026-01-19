Select 'PBMPC290-10'||','||
coalesce(cast(A.Count1 as varchar(20)),0) ||','||
trim(coalesce(cast(A.Exp_Gross_Rbmt_Crnt_Amt as varchar(20)),0)) ||','||
trim(coalesce(cast(A.Exp_Cont_Alw_Crnt_Amt as varchar(20)),0)) ||','||
trim(coalesce(cast(A.Exp_Payment_Crnt_Amt as varchar(20)),0)) ||','||
trim(coalesce(cast(A.Exp_Charge_Crnt_Amt as varchar(20)),0)) ||','||
trim(coalesce(cast(A.Var_Gross_Rbmt_New_Amt as varchar(20)),0)) ||','||
trim(coalesce(cast(A.Var_Cont_Alw_New_Amt as varchar(20)),0)) ||','||
trim(coalesce(cast(A.Var_Payment_New_Amt as varchar(20)),0)) ||','||
trim(coalesce(cast(A.Var_Charge_New_Amt as varchar(20)),0)) ||',' as Source_String 
From(
Select count(*) as Count1, sum(Exp_Gross_Rbmt_Crnt_Amt) as Exp_Gross_Rbmt_Crnt_Amt,
sum(Exp_Cont_Alw_Crnt_Amt) as Exp_Cont_Alw_Crnt_Amt,
sum(Exp_Payment_Crnt_Amt) as Exp_Payment_Crnt_Amt,
sum(Exp_Charge_Crnt_Amt) as Exp_Charge_Crnt_Amt,
sum(Var_Gross_Rbmt_New_Amt) as Var_Gross_Rbmt_New_Amt,
sum(Var_Cont_Alw_New_Amt) as Var_Cont_Alw_New_Amt,
sum(Var_Payment_New_Amt) as Var_Payment_New_Amt,
sum(Var_Charge_New_Amt) as Var_Charge_New_Amt 
From edwpbs.Fact_RCOM_PARS_Discrepancy 
where Date_Sid = Cast(cast((add_months(current_date, -1) (format 'yyyymm')) as char(6)) as integer)and Discrepancy_Resolved_Date  <> '0001-01-01'
AND coid not in (select coid from edwpbs.parallon_client_detail where Company_code='CHP') )A 