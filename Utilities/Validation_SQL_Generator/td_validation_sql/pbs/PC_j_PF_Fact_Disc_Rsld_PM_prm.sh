#@@START DMEXPRESS_EXPORTED_VARIABLES

export AC_EXP_TOLERANCE_PERCENT=0
export AC_EXP_TOLERANCE_AMT=0
export JOBNAME='PBMPC290-20'

logon=".run file /etl/ST/PBS/PC/LOGON/EDWPBS_LOGON"

. /etl/jfmd/PBS/PC/InfobatScripts/DMX_VARS.sh
select_statement="select RunDate from edwpbs_staging.stg_run_date where extract(day from rundate)=1;"
logoff=".LOGOFF"

echo $logon > /etl/jfmd/PBS/PC/Scripts/PBMPC290_20.sql
echo ".export report file=/etl/jfmd/PBS/PC/Scripts/PBMPC290_20.txt;" >> /etl/jfmd/PBS/PC/Scripts/PBMPC290_20.sql
echo $select_statement >> /etl/jfmd/PBS/PC/Scripts/PBMPC290_20.sql
echo ".export reset;" >> /etl/jfmd/PBS/PC/Scripts/PBMPC290_20.sql
echo $logoff >> /etl/jfmd/PBS/PC/Scripts/PBMPC290_20.sql
echo ".QUIT" >> /etl/jfmd/PBS/PC/Scripts/PBMPC290_20.sql
chmod 777 /etl/jfmd/PBS/PC/Scripts/PBMPC290_20.sql
##chmod 777 /etl/jfmd/PBS/PC/Scripts/PBMPC290_20.txt


###  Bteq call to run the insert query ###
bteq < /etl/jfmd/PBS/PC/Scripts/PBMPC290_20.sql >>/dev/null
RC=$?
if [ $RC = 0 ] 
then
   rm /etl/jfmd/PBS/PC/Scripts/PBMPC290_20.sql
else
   echo "PBMPC290_20.sql executed Failed"
   exit 99
fi

sed '1,2d' /etl/jfmd/PBS/PC/Scripts/PBMPC290_20.txt > /etl/jfmd/PBS/PC/Scripts/PBMPC290_20_new.txt
count1=$(wc -l </etl/jfmd/PBS/PC/Scripts/PBMPC290_20_new.txt)
rm /etl/jfmd/PBS/PC/Scripts/PBMPC290_20.txt

echo "$count1"
if [ $count1 != 0 ]
then

export AC_EXP_SQL_STATEMENT="
Select 'PBMPC290-20'||','||
trim(cast(zeroifnull(A.RowCount) as varchar(10))) ||','||
trim(cast(zeroifnull(A.EOR_Gross_Reimbursement_Amt) as decimal(20,3))) ||','||
trim(cast(zeroifnull(A.EOR_Contractual_Allowance_Amt) as decimal(20,3))) ||','||
trim(cast(zeroifnull(A.EOR_Insurance_Payment_Amt) as decimal(20,3))) ||','||
trim(cast(zeroifnull(A.Sum1) as decimal(20,3))) ||','||
trim(cast(zeroifnull(A.Var_Gross_Reimbursement_Amt) as decimal(20,3))) ||','||
trim(cast(zeroifnull(A.Var_Contractual_Allowance_Amt) as decimal(20,3))) ||','||
trim(cast(zeroifnull(A.Var_Insurance_Payment_Amt) as decimal(20,3))) ||','||
trim(cast(zeroifnull(A.Var_Net_Billed_Charge_Amt) as decimal(20,3))) ||',' as SOURCE_STRING from(

SELECT 
count(RSVD.Patient_DW_ID) as RowCount,
Cast(sum(EOR.EOR_Gross_Reimbursement_Amt) as varchar(20)) as EOR_Gross_Reimbursement_Amt , 
Cast(sum(EOR.EOR_Contractual_Allowance_Amt) as varchar(20)) as EOR_Contractual_Allowance_Amt, 
Cast(sum(EOR.EOR_Insurance_Payment_Amt) as varchar(20)) as EOR_Insurance_Payment_Amt, 
Cast(sum(0) as varchar(20)) as Sum1, 
Cast(sum(ABS(RSVD.Var_Gross_Reimbursement_Amt)) as varchar(20)) as Var_Gross_Reimbursement_Amt, 
Cast(sum(ABS(RSVD.Var_Contractual_Allowance_Amt)) as varchar(20)) as Var_Contractual_Allowance_Amt, 
Cast(sum(ABS(RSVD.Var_Insurance_Payment_Amt)) as varchar(20)) as Var_Insurance_Payment_Amt, 
Cast(sum(ABS(RSVD.Var_Net_Billed_Charge_Amt)) as varchar(20)) as Var_Net_Billed_Charge_Amt


FROM  Edwpf_Base_Views.Resolved_Discrepancy RSVD

Left outer join Edwpf_Base_Views.Explanation_Of_Reimbursement EOR
ON RSVD.Company_Code = EOR.Company_Code
AND RSVD.Coid = EOR.Coid
AND RSVD.Patient_DW_ID = EOR.Patient_DW_ID 
AND RSVD.Payor_DW_ID = EOR.Payor_DW_ID
AND RSVD.IPLAN_Insurance_Order_Num = EOR.IPLAN_Insurance_Order_Num 
AND RSVD.EOR_Log_Date = EOR.EOR_Log_Date
AND RSVD.Log_ID = EOR.Log_ID 
AND RSVD.Log_Sequence_Num = EOR.Log_Sequence_Num
AND COALESCE(RSVD.CC_Calc_Id,999) = COALESCE(EOR.CC_Calc_Id,999)


Left outer join Edwpf_Base_Views.Fact_RCOM_PARS_Discrepancy DISC
ON DISC.Date_Sid = cast((add_months(current_date, -1) (format 'yyyymm')) as char(6))
AND DISC.Company_Code = RSVD.Company_Code
AND DISC.Coid = RSVD.Coid
AND DISC.Patient_DW_ID = RSVD.Patient_DW_ID 
AND DISC.Payor_DW_ID = RSVD.Payor_DW_ID
AND DISC.IPLAN_Insurance_Order_Num = RSVD.IPLAN_Insurance_Order_Num 
AND DISC.EOR_Log_Date = RSVD.EOR_Log_Date
AND DISC.Log_Sequence_Num = RSVD.Log_Sequence_Num
AND DISC.AR_Bill_Thru_Date  = EOR.AR_Bill_Thru_Date
AND DISC.Discrepancy_Creation_Date = RSVD.Discrepancy_Origination_Date
AND DISC.Log_ID = RSVD.Log_ID 
AND DISC.Discrepancy_Resolved_Date  = '0001-01-01'

Left outer join 
(Select PT.Patient_Dw_Id, Max(PT.Eff_From_Date)
From Edwpf_Base_Views.Admission_Patient_Type PT Group by 1) ATP
(Patient_Dw_Id,  Eff_From_Date)
on ATP.Patient_DW_Id = RSVD.Patient_DW_Id

Left outer join Edwpf_Base_Views.Admission_Patient_Type APT
on APT.Patient_DW_Id = RSVD.Patient_DW_Id
and APT.Eff_From_Date = ATP.Eff_From_Date

Left outer join Edwpf_Base_Views.Admission_Discharge AD
on AD.Patient_DW_Id = RSVD.Patient_DW_Id

Left outer join Edwfs_Base_Views.FACILITY FF
on FF.Coid = RSVD.Coid
and FF.Company_Code = RSVD.Company_Code

WHERE  
cast((RSVD.Discrepancy_Resolved_Date (format 'yyyymm')) as char(6)) =  cast((add_months(current_date, -1) (format 'yyyymm')) as char(6))
AND	
cast((RSVD.Discrepancy_Origination_Date (format 'yyyymm')) as char(6)) <  cast((add_months(current_date, -1) (format 'yyyymm')) as char(6))

AND EOR.Eff_From_Date = 
(Select Max(ER.Eff_From_Date) From Edwpf_Base_Views.Explanation_Of_Reimbursement ER
Where RSVD.Company_Code = ER.Company_Code
AND RSVD.Coid = ER.Coid
AND RSVD.Patient_DW_ID = ER.Patient_DW_ID 
AND RSVD.Payor_DW_ID = ER.Payor_DW_ID
AND RSVD.IPLAN_Insurance_Order_Num = ER.IPLAN_Insurance_Order_Num 
AND RSVD.EOR_Log_Date = ER.EOR_Log_Date
AND RSVD.Log_ID = ER.Log_ID 
AND RSVD.Log_Sequence_Num = ER.Log_Sequence_Num
AND COALESCE(RSVD.CC_Calc_Id,999) = COALESCE(ER.CC_Calc_Id,999)

AND ER.Eff_From_Date <= RSVD.Discrepancy_Resolved_Date)
AND RSVD.Coid in (select coid from edwpbs.parallon_client_detail where Company_code='CHP'))A"

export AC_ACT_SQL_STATEMENT="Locking table Edwpbs.Fact_RCOM_PARS_Discrepancy for access
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
AND coid in (select coid from edwpbs.parallon_client_detail where Company_code='CHP'))A"

echo "if part"
rm /etl/jfmd/PBS/PC/Scripts/PBMPC290_20_new.txt
##exit 0

else

export AC_EXP_SQL_STATEMENT="Select 'PBMPC290-20'||','||
trim(cast(zeroifnull(A.RowCount) as varchar(10))) ||','||
trim(cast(zeroifnull(A.EOR_Gross_Reimbursement_Amt) as decimal(20,3))) ||','||
trim(cast(zeroifnull(A.EOR_Contractual_Allowance_Amt) as decimal(20,3))) ||','||
trim(cast(zeroifnull(A.EOR_Insurance_Payment_Amt) as decimal(20,3))) ||','||
trim(cast(zeroifnull(A.Sum1) as decimal(20,3))) ||','||
trim(cast(zeroifnull(A.Var_Gross_Reimbursement_Amt) as decimal(20,3))) ||','||
trim(cast(zeroifnull(A.Var_Contractual_Allowance_Amt) as decimal(20,3))) ||','||
trim(cast(zeroifnull(A.Var_Insurance_Payment_Amt) as decimal(20,3))) ||','||
trim(cast(zeroifnull(A.Var_Net_Billed_Charge_Amt) as decimal(20,3))) ||',' as SOURCE_STRING from(

SELECT 
count(RSVD.Patient_DW_ID) as RowCount,
Cast(sum(EOR.EOR_Gross_Reimbursement_Amt) as varchar(20)) as EOR_Gross_Reimbursement_Amt , 
Cast(sum(EOR.EOR_Contractual_Allowance_Amt) as varchar(20)) as EOR_Contractual_Allowance_Amt, 
Cast(sum(EOR.EOR_Insurance_Payment_Amt) as varchar(20)) as EOR_Insurance_Payment_Amt, 
Cast(sum(0) as varchar(20)) as Sum1, 
Cast(sum(ABS(RSVD.Var_Gross_Reimbursement_Amt)) as varchar(20)) as Var_Gross_Reimbursement_Amt, 
Cast(sum(ABS(RSVD.Var_Contractual_Allowance_Amt)) as varchar(20)) as Var_Contractual_Allowance_Amt, 
Cast(sum(ABS(RSVD.Var_Insurance_Payment_Amt)) as varchar(20)) as Var_Insurance_Payment_Amt, 
Cast(sum(ABS(RSVD.Var_Net_Billed_Charge_Amt)) as varchar(20)) as Var_Net_Billed_Charge_Amt


FROM  Edwpf_Base_Views.Resolved_Discrepancy RSVD

Left outer join Edwpf_Base_Views.Explanation_Of_Reimbursement EOR
ON RSVD.Company_Code = EOR.Company_Code
AND RSVD.Coid = EOR.Coid
AND RSVD.Patient_DW_ID = EOR.Patient_DW_ID 
AND RSVD.Payor_DW_ID = EOR.Payor_DW_ID
AND RSVD.IPLAN_Insurance_Order_Num = EOR.IPLAN_Insurance_Order_Num 
AND RSVD.EOR_Log_Date = EOR.EOR_Log_Date
AND RSVD.Log_ID = EOR.Log_ID 
AND RSVD.Log_Sequence_Num = EOR.Log_Sequence_Num
AND COALESCE(RSVD.CC_Calc_Id,999) = COALESCE(EOR.CC_Calc_Id,999)

Left outer join Edwpf_Base_Views.Fact_RCOM_PARS_Discrepancy DISC
ON DISC.Date_Sid = cast((add_months(current_date, -1) (format 'yyyymm')) as char(6))
AND DISC.Company_Code = RSVD.Company_Code
AND DISC.Coid = RSVD.Coid
AND DISC.Patient_DW_ID = RSVD.Patient_DW_ID 
AND DISC.Payor_DW_ID = RSVD.Payor_DW_ID
AND DISC.IPLAN_Insurance_Order_Num = RSVD.IPLAN_Insurance_Order_Num 
AND DISC.EOR_Log_Date = RSVD.EOR_Log_Date
AND DISC.Log_Sequence_Num = RSVD.Log_Sequence_Num
AND DISC.AR_Bill_Thru_Date  = EOR.AR_Bill_Thru_Date
AND DISC.Discrepancy_Creation_Date = RSVD.Discrepancy_Origination_Date
AND DISC.Log_ID = RSVD.Log_ID 
AND DISC.Discrepancy_Resolved_Date  = '0001-01-01'

Left outer join 
(Select PT.Patient_Dw_Id, Max(PT.Eff_From_Date)
From Edwpf_Base_Views.Admission_Patient_Type PT Group by 1) ATP
(Patient_Dw_Id,  Eff_From_Date)
on ATP.Patient_DW_Id = RSVD.Patient_DW_Id

Left outer join Edwpf_Base_Views.Admission_Patient_Type APT
on APT.Patient_DW_Id = RSVD.Patient_DW_Id
and APT.Eff_From_Date = ATP.Eff_From_Date

Left outer join Edwpf_Base_Views.Admission_Discharge AD
on AD.Patient_DW_Id = RSVD.Patient_DW_Id

Left outer join Edwfs_Base_Views.FACILITY FF
on FF.Coid = RSVD.Coid
and FF.Company_Code = RSVD.Company_Code

WHERE  
cast((RSVD.Discrepancy_Resolved_Date (format 'yyyymm')) as char(6)) =  cast((add_months(current_date, -1) (format 'yyyymm')) as char(6))
AND	
cast((RSVD.Discrepancy_Origination_Date (format 'yyyymm')) as char(6)) <  cast((add_months(current_date, -1) (format 'yyyymm')) as char(6))

AND EOR.Eff_From_Date = 
(Select Max(ER.Eff_From_Date) From Edwpf_Base_Views.Explanation_Of_Reimbursement ER
Where RSVD.Company_Code = ER.Company_Code
AND RSVD.Coid = ER.Coid
AND RSVD.Patient_DW_ID = ER.Patient_DW_ID 
AND RSVD.Payor_DW_ID = ER.Payor_DW_ID
AND RSVD.IPLAN_Insurance_Order_Num = ER.IPLAN_Insurance_Order_Num 
AND RSVD.EOR_Log_Date = ER.EOR_Log_Date
AND RSVD.Log_ID = ER.Log_ID 
AND RSVD.Log_Sequence_Num = ER.Log_Sequence_Num
AND COALESCE(RSVD.CC_Calc_Id,999) = COALESCE(ER.CC_Calc_Id,999)

AND ER.Eff_From_Date <= RSVD.Discrepancy_Resolved_Date)
AND RSVD.Coid not in (select coid from edwpbs.parallon_client_detail where Company_code='CHP'))A"

export AC_ACT_SQL_STATEMENT="Locking table Edwpbs.Fact_RCOM_PARS_Discrepancy for access
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
AND coid not in (select coid from edwpbs.parallon_client_detail where Company_code='CHP'))A"
echo "else part"
rm /etl/jfmd/PBS/PC/Scripts/PBMPC290_20_new.txt
##exit 0
fi


#  @@END DMEXPRESS_EXPORTED_VARIABLES
#  End of Exported variables section
#  
