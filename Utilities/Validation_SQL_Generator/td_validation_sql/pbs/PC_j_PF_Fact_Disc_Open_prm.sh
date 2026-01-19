
export AC_EXP_TOLERANCE_PERCENT=0
export AC_EXP_TOLERANCE_AMT=0
export JOBNAME='PBMPC290-30'


logon=".run file /etl/ST/PBS/PC/LOGON/EDWPBS_LOGON"

. /etl/jfmd/PBS/PC/InfobatScripts/DMX_VARS.sh
select_statement="select RunDate from edwpbs_staging.stg_run_date where extract(day from rundate)=1;"
logoff=".LOGOFF"

echo $logon > /etl/jfmd/PBS/PC/Scripts/PBMPC290_30.sql
echo ".export report file=/etl/jfmd/PBS/PC/Scripts/PBMPC290_30.txt;" >> /etl/jfmd/PBS/PC/Scripts/PBMPC290_30.sql
echo $select_statement >> /etl/jfmd/PBS/PC/Scripts/PBMPC290_30.sql
echo ".export reset;" >> /etl/jfmd/PBS/PC/Scripts/PBMPC290_30.sql
echo $logoff >> /etl/jfmd/PBS/PC/Scripts/PBMPC290_30.sql
echo ".QUIT" >> /etl/jfmd/PBS/PC/Scripts/PBMPC290_30.sql
chmod 777 /etl/jfmd/PBS/PC/Scripts/PBMPC290_30.sql
##chmod 777 /etl/jfmd/PBS/PC/Scripts/PBMPC290_30.txt


###  Bteq call to run the insert query ###
bteq < /etl/jfmd/PBS/PC/Scripts/PBMPC290_30.sql >>/dev/null
RC=$?
if [ $RC = 0 ] 
then
   rm /etl/jfmd/PBS/PC/Scripts/PBMPC290_30.sql
else
   echo "PBMPC290_30.sql executed Failed"
   exit 99
fi

sed '1,2d' /etl/jfmd/PBS/PC/Scripts/PBMPC290_30.txt > /etl/jfmd/PBS/PC/Scripts/PBMPC290_30_new.txt
count1=$(wc -l </etl/jfmd/PBS/PC/Scripts/PBMPC290_30_new.txt)
rm /etl/jfmd/PBS/PC/Scripts/PBMPC290_30.txt

echo "$count1"
if [ $count1 != 0 ]
then

export AC_EXP_SQL_STATEMENT="SELECT 
'PBMPC290-30' || ','  ||
'0'  || ','  ||
trim(coalesce(Cast(Sum(0) as varchar(20)),0))  || ','  ||
trim(coalesce(Cast(Sum(0) as varchar(20)),0))  || ','  ||
trim(coalesce(Cast(Sum(0) as varchar(20)),0))  || ','  ||
trim(coalesce(Cast(Sum(0) as varchar(20)),0))  || ','  ||
trim(coalesce(Cast(Sum(ABS(RD.Var_Gross_Reimbursement_Amt)) as varchar(20)),0))  || ','  ||
trim(coalesce(Cast(Sum(ABS(RD.Var_Contractual_Allowance_Amt)) as varchar(20)),0))  || ','  ||
trim(coalesce(Cast(Sum(ABS(RD.Var_Insurance_Payment_Amt)) as varchar(20)),0))  || ','  ||
trim(coalesce(Cast(Sum(ABS(RD.Var_Net_Billed_Charge_Amt)) as varchar(20)),0))  || ','  
AS SOURCE_STRING
FROM  Edwpf_Base_Views.Reimbursement_Discrepancy_EOM RD
Left outer join Edwpf_Base_Views.Explanation_Of_Reimbursement EOR
ON RD.Company_Code = EOR.Company_Code
AND RD.Coid = EOR.Coid AND RD.Patient_DW_ID = EOR.Patient_DW_ID 
AND RD.Payor_DW_ID = EOR.Payor_DW_ID AND RD.IPLAN_Insurance_Order_Num = EOR.IPLAN_Insurance_Order_Num 
AND RD.EOR_Log_Date = EOR.EOR_Log_Date AND RD.Log_ID = EOR.Log_ID 
AND RD.Log_Sequence_Num = EOR.Log_Sequence_Num
AND COALESCE(RD.CC_Calc_Id,999) = COALESCE(EOR.CC_Calc_Id,999)

WHERE EOR.Eff_From_Date = 
(Select Max(ER.Eff_From_Date) From Edwpf_Base_Views.Explanation_Of_Reimbursement ER
Where RD.Company_Code = ER.Company_Code
AND RD.Coid = ER.Coid
AND RD.Patient_DW_ID = ER.Patient_DW_ID 
AND RD.Payor_DW_ID = ER.Payor_DW_ID
AND RD.IPLAN_Insurance_Order_Num = ER.IPLAN_Insurance_Order_Num 
AND RD.EOR_Log_Date = ER.EOR_Log_Date
AND RD.Log_ID = ER.Log_ID 
AND RD.Log_Sequence_Num = ER.Log_Sequence_Num
AND COALESCE(RD.CC_Calc_Id,999) = COALESCE(ER.CC_Calc_Id,999)

AND ER.Eff_From_Date <= (cast((cast(((current_date) (format 'yyyy-mm')) as char(7)) || '-01') as date) -1))
AND RD.Coid in (select coid from edwpbs.parallon_client_detail where Company_code='CHP')"

export AC_ACT_SQL_STATEMENT="Locking table edwpbs.Fact_RCOM_PARS_Discrepancy for access
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
AND coid in (select coid from edwpbs.parallon_client_detail where Company_code='CHP')"

echo "if part"
rm /etl/jfmd/PBS/PC/Scripts/PBMPC290_30_new.txt
##exit 0

else

export AC_EXP_SQL_STATEMENT="SELECT 
'PBMPC290-30' || ','  ||
'0'  || ','  ||
trim(coalesce(Cast(Sum(0) as varchar(20)),0))  || ','  ||
trim(coalesce(Cast(Sum(0) as varchar(20)),0))  || ','  ||
trim(coalesce(Cast(Sum(0) as varchar(20)),0))  || ','  ||
trim(coalesce(Cast(Sum(0) as varchar(20)),0))  || ','  ||
trim(coalesce(Cast(Sum(ABS(RD.Var_Gross_Reimbursement_Amt)) as varchar(20)),0))  || ','  ||
trim(coalesce(Cast(Sum(ABS(RD.Var_Contractual_Allowance_Amt)) as varchar(20)),0))  || ','  ||
trim(coalesce(Cast(Sum(ABS(RD.Var_Insurance_Payment_Amt)) as varchar(20)),0))  || ','  ||
trim(coalesce(Cast(Sum(ABS(RD.Var_Net_Billed_Charge_Amt)) as varchar(20)),0))  || ','  
AS SOURCE_STRING
FROM  Edwpf_Base_Views.Reimbursement_Discrepancy_EOM RD
Left outer join Edwpf_Base_Views.Explanation_Of_Reimbursement EOR
ON RD.Company_Code = EOR.Company_Code
AND RD.Coid = EOR.Coid AND RD.Patient_DW_ID = EOR.Patient_DW_ID 
AND RD.Payor_DW_ID = EOR.Payor_DW_ID AND RD.IPLAN_Insurance_Order_Num = EOR.IPLAN_Insurance_Order_Num 
AND RD.EOR_Log_Date = EOR.EOR_Log_Date AND RD.Log_ID = EOR.Log_ID 
AND RD.Log_Sequence_Num = EOR.Log_Sequence_Num
AND COALESCE(RD.CC_Calc_Id,999) = COALESCE(EOR.CC_Calc_Id,999)

WHERE EOR.Eff_From_Date = 
(Select Max(ER.Eff_From_Date) From Edwpf_Base_Views.Explanation_Of_Reimbursement ER
Where RD.Company_Code = ER.Company_Code
AND RD.Coid = ER.Coid
AND RD.Patient_DW_ID = ER.Patient_DW_ID 
AND RD.Payor_DW_ID = ER.Payor_DW_ID
AND RD.IPLAN_Insurance_Order_Num = ER.IPLAN_Insurance_Order_Num 
AND RD.EOR_Log_Date = ER.EOR_Log_Date
AND RD.Log_ID = ER.Log_ID 
AND RD.Log_Sequence_Num = ER.Log_Sequence_Num
AND COALESCE(RD.CC_Calc_Id,999) = COALESCE(ER.CC_Calc_Id,999)

AND ER.Eff_From_Date <= (cast((cast(((current_date) (format 'yyyy-mm')) as char(7)) || '-01') as date) -1))
AND RD.Coid not in (select coid from edwpbs.parallon_client_detail where Company_code='CHP')"

export AC_ACT_SQL_STATEMENT="Locking table edwpbs.Fact_RCOM_PARS_Discrepancy for access
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
AND coid not in (select coid from edwpbs.parallon_client_detail where Company_code='CHP')"

echo "else part"
rm /etl/jfmd/PBS/PC/Scripts/PBMPC290_30_new.txt
##exit 0
fi

