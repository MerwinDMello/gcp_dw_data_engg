#  @@START DMEXPRESS_EXPORTED_VARIABLES


export AC_EXP_TOLERANCE_PERCENT=0
export AC_EXP_TOLERANCE_AMT=0
export JOBNAME='PBMPC290-40'
export PE_DATE='9999-12-31' 

logon=".run file /etl/ST/PBS/PC/LOGON/EDWPBS_LOGON"

. /etl/jfmd/PBS/PC/InfobatScripts/DMX_VARS.sh
select_statement="select RunDate from edwpbs_staging.stg_run_date where extract(day from rundate)=1;"
logoff=".LOGOFF"

echo $logon > /etl/jfmd/PBS/PC/Scripts/PBMPC290-40.sql
echo ".export report file=/etl/jfmd/PBS/PC/Scripts/PBMPC290-40.txt;" >> /etl/jfmd/PBS/PC/Scripts/PBMPC290-40.sql
echo $select_statement >> /etl/jfmd/PBS/PC/Scripts/PBMPC290-40.sql
echo ".export reset;" >> /etl/jfmd/PBS/PC/Scripts/PBMPC290-40.sql
echo $logoff >> /etl/jfmd/PBS/PC/Scripts/PBMPC290-40.sql
echo ".QUIT" >> /etl/jfmd/PBS/PC/Scripts/PBMPC290-40.sql
chmod 777 /etl/jfmd/PBS/PC/Scripts/PBMPC290-40.sql
##chmod 777 /etl/jfmd/PBS/PC/Scripts/PBMPC290-40.txt


###  Bteq call to run the insert query ###
bteq < /etl/jfmd/PBS/PC/Scripts/PBMPC290-40.sql >>/dev/null
RC=$?
if [ $RC = 0 ] 
then
   rm /etl/jfmd/PBS/PC/Scripts/PBMPC290-40.sql
else
   echo "PBMPC290-40.sql executed Failed"
   exit 99
fi

sed '1,2d' /etl/jfmd/PBS/PC/Scripts/PBMPC290-40.txt > /etl/jfmd/PBS/PC/Scripts/PBMPC290-40_new.txt
count1=$(wc -l </etl/jfmd/PBS/PC/Scripts/PBMPC290-40_new.txt)
rm /etl/jfmd/PBS/PC/Scripts/PBMPC290-40.txt

echo "$count1"
if [ $count1 != 0 ]
then

export AC_ACT_SQL_STATEMENT="SELECT 'PBMPC290-40' || ',' || coalesce(trim(Count(*)),'0') || ',' || 
coalesce(trim(Cast( Sum(zeroifnull(DISC.Var_Gross_Rbmt_Othr_Cor_Amt) ) AS VARCHAR(20))),'0') || ',' || 
coalesce(trim(Cast( Sum(zeroifnull(DISC.Var_Cont_Alw_Othr_Cor_Amt)) AS VARCHAR(20))),'0') || ',' || 
coalesce(trim(Cast( Sum(zeroifnull(DISC.Var_Payment_Othr_Cor_Amt) ) AS VARCHAR(20))),'0') || ',' || 
coalesce(trim(Cast( Sum(zeroifnull(DISC.Var_Charge_Othr_Cor_Amt) ) AS VARCHAR(20))),'0') || ',' as Source_String 
FROM EDWPBS.Fact_RCOM_PARS_Discrepancy DISC 
where DISC.Date_Sid = cast((add_months(current_date, -1) (format 'yyyymm')) as char(6))
AND coid IN (select coid from edwpbs.parallon_client_detail where Company_code='CHP')"

export AC_EXP_SQL_STATEMENT="SELECT 'PBMPC290-40' || ',' || coalesce(trim(Count(DISC.Patient_DW_ID)),'0') || ',' || 
coalesce(trim(Cast( Sum(zeroifnull(DISC.Var_Gross_Rbmt_Beg_Amt) + 
zeroifnull(DISC.Var_Gross_Rbmt_New_Amt) - 
zeroifnull(DISC.Var_Gross_Rbmt_Resolved_Amt )-  
zeroifnull( Case  WHEN Reason_Code_1_Sid IN ( 28, 56, 136 ,1001 , 1002 ,1003 , 1004 , 1005 ,965)
OR Reason_Code_2_Sid IN ( 28, 56, 136 ,1001 , 1002 ,1003 , 1004 , 1005 ,965 ) OR 
Reason_Code_3_Sid IN ( 28, 56, 136 ,1001 , 1002 ,1003 , 1004 , 1005 ,965 ) OR 
Reason_Code_4_Sid IN ( 28, 56, 136 ,1001 , 1002 ,1003 , 1004 , 1005 ,965 )
Then 0.000 Else DISC.Var_Gross_Rbmt_End_Amt End)) AS VARCHAR(20))),'0') || ',' || 
coalesce(trim(Cast( Sum(zeroifnull(DISC.Var_Cont_Alw_Beg_Amt)  + 
zeroifnull(DISC.Var_Cont_Alw_New_Amt) - zeroifnull(DISC.Var_Cont_Alw_Resolved_Amt) - 
ZEROIFNULL(CASE WHEN Reason_Code_1_Sid IN ( 28, 56, 136 ,1001 , 1002 ,1003 , 1004 , 1005 ,965 )
OR Reason_Code_2_Sid IN ( 28, 56, 136 ,1001 , 1002 ,1003 , 1004 , 1005 ,965 ) OR 
Reason_Code_3_Sid IN ( 28, 56, 136 ,1001 , 1002 ,1003 , 1004 , 1005 ,965 ) OR 
Reason_Code_4_Sid IN ( 28, 56, 136 ,1001 , 1002 ,1003 , 1004 , 1005 ,965 )
Then 0.000 Else DISC.Var_Cont_Alw_End_Amt End)) AS VARCHAR(20))),'0') || ',' || 
coalesce(trim(Cast( Sum(zeroifnull(DISC.Var_Payment_Beg_Amt) + zeroifnull(DISC.Var_Payment_New_Amt) - 
ZEROIFNULL(DISC.Var_Payment_Resolved_Amt) - ZEROIFNULL(CASE WHEN   Reason_Code_1_Sid IN ( 28, 56, 136 ,1001 , 1002 ,1003 , 1004 , 1005 ,965 )
OR Reason_Code_2_Sid IN ( 28, 56, 136 ,1001 , 1002 ,1003 , 1004 , 1005 ,965 ) OR 
Reason_Code_3_Sid IN ( 28, 56, 136 ,1001 , 1002 ,1003 , 1004 , 1005 ,965 ) OR 
Reason_Code_4_Sid IN ( 28, 56, 136 ,1001 , 1002 ,1003 , 1004 , 1005 ,965 ) 
 Then 0.000 Else DISC.Var_Payment_End_Amt End)) AS VARCHAR(20))),'0') || ',' || 
 coalesce(trim(Cast( Sum(zeroifnull(DISC.Var_Charge_Beg_Amt) + zeroifnull(DISC.Var_Charge_New_Amt) - 
 zeroifnull(DISC.Var_Charge_Resolved_Amt) - zeroifnull(Case  WHEN Reason_Code_1_Sid IN ( 28, 56, 136 ,1001 , 1002 ,1003 , 1004 , 1005  ,965)
OR Reason_Code_2_Sid IN ( 28, 56, 136 ,1001 , 1002 ,1003 , 1004 , 1005 ,965 ) OR 
Reason_Code_3_Sid IN ( 28, 56, 136 ,1001 , 1002 ,1003 , 1004 , 1005 ,965 ) OR 
Reason_Code_4_Sid IN ( 28, 56, 136 ,1001 , 1002 ,1003 , 1004 , 1005 ,965 ) 
 Then 0.00 Else DISC.Var_Charge_End_Amt End)) AS VARCHAR(20))),'0') || ',' as Source_String 
 FROM  Edwpbs.Fact_RCOM_PARS_Discrepancy DISC 
Where  
 DISC.Date_Sid = cast((add_months(current_date, -1) (format 'yyyymm')) as char(6)) and disc.patient_sid <> 999999999999
AND DISC.Coid IN (SELECT coid FROM edwpbs.parallon_client_detail WHERE Company_code='CHP')
"

echo "if part"
rm /etl/jfmd/PBS/PC/Scripts/PBMPC290-40_new.txt
##exit 0

else

export AC_ACT_SQL_STATEMENT="SELECT 'PBMPC290-40' || ',' || coalesce(trim(Count(*)),'0') || ',' || 
coalesce(trim(Cast( Sum(zeroifnull(DISC.Var_Gross_Rbmt_Othr_Cor_Amt) ) AS VARCHAR(20))),'0') || ',' || 
coalesce(trim(Cast( Sum(zeroifnull(DISC.Var_Cont_Alw_Othr_Cor_Amt)) AS VARCHAR(20))),'0') || ',' || 
coalesce(trim(Cast( Sum(zeroifnull(DISC.Var_Payment_Othr_Cor_Amt) ) AS VARCHAR(20))),'0') || ',' || 
coalesce(trim(Cast( Sum(zeroifnull(DISC.Var_Charge_Othr_Cor_Amt) ) AS VARCHAR(20))),'0') || ',' as Source_String 
FROM EDWPBS.Fact_RCOM_PARS_Discrepancy DISC 
where DISC.Date_Sid = cast((add_months(current_date, -1) (format 'yyyymm')) as char(6))
AND coid NOT IN (select coid from edwpbs.parallon_client_detail where Company_code='CHP')"

export AC_EXP_SQL_STATEMENT="SELECT 'PBMPC290-40' || ',' || coalesce(trim(Count(DISC.Patient_DW_ID)),'0') || ',' || 
coalesce(trim(Cast( Sum(zeroifnull(DISC.Var_Gross_Rbmt_Beg_Amt) + 
zeroifnull(DISC.Var_Gross_Rbmt_New_Amt) - 
zeroifnull(DISC.Var_Gross_Rbmt_Resolved_Amt )-  
zeroifnull( Case  WHEN Reason_Code_1_Sid IN ( 28, 56, 136 ,1001 , 1002 ,1003 , 1004 , 1005 ,965 )
OR Reason_Code_2_Sid IN ( 28, 56, 136 ,1001 , 1002 ,1003 , 1004 , 1005 ,965 ) OR 
Reason_Code_3_Sid IN ( 28, 56, 136 ,1001 , 1002 ,1003 , 1004 , 1005 ,965 ) OR 
Reason_Code_4_Sid IN ( 28, 56, 136 ,1001 , 1002 ,1003 , 1004 , 1005 ,965 )
Then 0.000 Else DISC.Var_Gross_Rbmt_End_Amt End)) AS VARCHAR(20))),'0') || ',' || 
coalesce(trim(Cast( Sum(zeroifnull(DISC.Var_Cont_Alw_Beg_Amt)  + 
zeroifnull(DISC.Var_Cont_Alw_New_Amt) - zeroifnull(DISC.Var_Cont_Alw_Resolved_Amt) - 
ZEROIFNULL(CASE WHEN Reason_Code_1_Sid IN ( 28, 56, 136 ,1001 , 1002 ,1003 , 1004 , 1005 ,965 )
OR Reason_Code_2_Sid IN ( 28, 56, 136 ,1001 , 1002 ,1003 , 1004 , 1005  ,965) OR 
Reason_Code_3_Sid IN ( 28, 56, 136 ,1001 , 1002 ,1003 , 1004 , 1005 ,965 ) OR 
Reason_Code_4_Sid IN ( 28, 56, 136 ,1001 , 1002 ,1003 , 1004 , 1005 ,965 )
Then 0.000 Else DISC.Var_Cont_Alw_End_Amt End)) AS VARCHAR(20))),'0') || ',' || 
coalesce(trim(Cast( Sum(zeroifnull(DISC.Var_Payment_Beg_Amt) + zeroifnull(DISC.Var_Payment_New_Amt) - 
ZEROIFNULL(DISC.Var_Payment_Resolved_Amt) - ZEROIFNULL(CASE WHEN   Reason_Code_1_Sid IN ( 28, 56, 136 ,1001 , 1002 ,1003 , 1004 , 1005 ,965 )
OR Reason_Code_2_Sid IN ( 28, 56, 136 ,1001 , 1002 ,1003 , 1004 , 1005 ,965 ) OR 
Reason_Code_3_Sid IN ( 28, 56, 136 ,1001 , 1002 ,1003 , 1004 , 1005 ,965 ) OR 
Reason_Code_4_Sid IN ( 28, 56, 136 ,1001 , 1002 ,1003 , 1004 , 1005 ,965 ) 
 Then 0.000 Else DISC.Var_Payment_End_Amt End)) AS VARCHAR(20))),'0') || ',' || 
 coalesce(trim(Cast( Sum(zeroifnull(DISC.Var_Charge_Beg_Amt) + zeroifnull(DISC.Var_Charge_New_Amt) - 
 zeroifnull(DISC.Var_Charge_Resolved_Amt) - zeroifnull(Case  WHEN Reason_Code_1_Sid IN ( 28, 56, 136 ,1001 , 1002 ,1003 , 1004 , 1005 ,965 )
OR Reason_Code_2_Sid IN ( 28, 56, 136 ,1001 , 1002 ,1003 , 1004 , 1005 ,965 ) OR 
Reason_Code_3_Sid IN ( 28, 56, 136 ,1001 , 1002 ,1003 , 1004 , 1005 ,965 ) OR 
Reason_Code_4_Sid IN ( 28, 56, 136 ,1001 , 1002 ,1003 , 1004 , 1005 ,965 ) 
 Then 0.00 Else DISC.Var_Charge_End_Amt End)) AS VARCHAR(20))),'0') || ',' as Source_String 
 FROM  Edwpbs.Fact_RCOM_PARS_Discrepancy DISC 
Where  
 DISC.Date_Sid = cast((add_months(current_date, -1) (format 'yyyymm')) as char(6)) and disc.patient_sid <> 999999999999
AND DISC.coid NOT IN (SELECT coid FROM edwpbs.parallon_client_detail WHERE Company_code='CHP')
"

echo "else part"
rm /etl/jfmd/PBS/PC/Scripts/PBMPC290-40_new.txt
##exit 0
fi

#  @@END DMEXPRESS_EXPORTED_VARIABLES
#  End of Exported variables section
#  
