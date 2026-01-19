Locking table Edwpbs.Fact_RCOM_PARS_Discrepancy for access
select 'PBMPC290-20'||','||cast(zeroifnull(A.counts) as varchar(20))||','||
cast(zeroifnull(A.Exp_Gross_Rbmt_Crnt_Amt) as varchar(20))||','||
cast(zeroifnull(A.Exp_Cont_Alw_Crnt_Amt) as varchar(20))||','||
cast(zeroifnull(A.Exp_Payment_Crnt_Amt) as varchar(20))||','||
cast(zeroifnull(A.Exp_Charge_Crnt_Amt) as varchar(20))||','||
cast(zeroifnull(A.Var_Gross_Rbmt_Resolved_Amt) as varchar(20))||','||
cast(zeroifnull(A.Var_Cont_Alw_Resolved_Amt) as varchar(20))||','||
cast(zeroifnull(A.Var_Payment_Resolved_Amt) as varchar(20))||','||
cast(zeroifnull(A.Var_Charge_Resolved_Amt) as varchar(20))||',' as Source_String from(
Select count(*) as counts, 
sum(Exp_Gross_Rbmt_Crnt_Amt) as Exp_Gross_Rbmt_Crnt_Amt,
sum(Exp_Cont_Alw_Crnt_Amt) as Exp_Cont_Alw_Crnt_Amt,
sum(Exp_Payment_Crnt_Amt) as Exp_Payment_Crnt_Amt,
sum(Exp_Charge_Crnt_Amt) as Exp_Charge_Crnt_Amt,
sum(Var_Gross_Rbmt_Resolved_Amt) as Var_Gross_Rbmt_Resolved_Amt,
sum(Var_Cont_Alw_Resolved_Amt) as Var_Cont_Alw_Resolved_Amt,
sum(Var_Payment_Resolved_Amt) as Var_Payment_Resolved_Amt,
sum(Var_Charge_Resolved_Amt) as Var_Charge_Resolved_Amt
From EDWPBS.Fact_RCOM_PARS_Discrepancy
where Date_Sid = Cast(cast((add_months(current_date, -1) (format 'yyyymm')) as char(6)) as integer)
AND cast((Discrepancy_Resolved_Date (format 'yyyymm')) as char(6)) =  cast((add_months(current_date, -1) (format 'yyyymm')) as char(6))
AND cast((Discrepancy_Creation_Date (format 'yyyymm')) as char(6)) <  cast((add_months(current_date, -1) (format 'yyyymm')) as char(6))
AND coid not in (select coid from edwpbs.parallon_client_detail where Company_code='CHP'))A