#  @@START DMEXPRESS_EXPORTED_VARIABLES

export AC_EXP_TOLERANCE_PERCENT=0
export AC_EXP_TOLERANCE_AMT=0
export JOBNAME='PBMPA100-20'
export PE_DATE='9999-12-31'

export AC_ACT_SQL_STATEMENT="SELECT 'PBMPA100-20' || ',' || 
coalesce(cast(trim(sum(sum1) )as varchar (20)),'0')|| ',' || 
coalesce(cast(trim(sum(sum2 ))as varchar (20)),'0') || ',' || 
coalesce(cast(trim(sum(sum3) )as varchar (20)),'0') || ',' as Source_string from 
(SELECT	SUM (Bad_Debt_Writeoff_Amt) as sum1,SUM(Non_Secn_Self_Pay_AR_Amt) as sum2,SUM(Non_Secn_Unins_Disc_Amt) as sum3 
FROM	edwpbs.Rpt_AR_ADA_Detail x WHERE  Month_Id =  CAST (  ( ADD_MONTHS(CURRENT_DATE, -1 ) (FORMAT 'YYYYMM') )  AS CHAR(6)) ) Src"


export AC_EXP_SQL_STATEMENT="SELECT 'PBMPA100-20' || ',' || 
TRIM(CAST(sum(k.Transaction_Amt)AS DECIMAL(15,2)))|| ',' || 
cast(trim(sum(Non_Sec_Self_pay_AR_AMT) )as varchar (20))|| ',' || 
cast(trim(sum(Non_Sec_Uninsured_discount) )as varchar (20))|| ',' as Source_string from(
sel EOM_END,SUM(Non_Sec_Self_pay_AR_AMT) AS Non_Sec_Self_pay_AR_AMT,SUM(Non_Sec_Uninsured_discount) AS Non_Sec_Uninsured_discount,SUM(Transaction_Amt) AS Transaction_Amt from (
SELECT CAST ( ( ( (CURRENT_DATE) (FORMAT 'YYYY-MM') )  || '-01'  )    AS DATE ) - 1 AS EOM_END	,
SUM(CASE	WHEN PEM.Account_Status_Code IN('AR','AX','CA') AND	CAST ((PEM.Discharge_Date (FORMAT 'YYYYMM')) AS CHAR(6)) <= PEM.RPTG_PERIOD			
AND	PEM.Total_Account_Balance_Amt > 0 THEN PEM.Patient_Balance_Amt 	ELSE 0 END) 	
 
+ SUM(CASE WHEN ((CAST((PEM.Discharge_Date (FORMAT 'YYYYMM')) AS CHAR(6)) > PEM.RPTG_PERIOD) OR (PEM.Account_Status_Code = 'UB' AND PEM.Discharge_Date IS NOT NULL AND 
CAST((PEM.Discharge_Date (FORMAT 'YYYYMM')) AS CHAR(6)) <= PEM.RPTG_PERIOD	AND Total_Account_Balance_Amt <> 0 )) 
AND Financial_Class_Code = 99 THEN Total_Account_Balance_Amt ELSE 0 END) + SUM(CASE WHEN PEM.Financial_Class_Code = 99 AND 
(( PEM.Account_Status_Code = 'UB' AND SUBSTR (PEM.Patient_Type_Code,2,1) <> 'P' AND PEM.Discharge_Date IS NULL	
AND PEM.Total_Account_Balance_Amt <> 0) OR ( PEM.Account_Status_Code = 'UB' AND PEM.Unbill_Pre_Admit_Ind = 'Y')) 
THEN PEM.Total_Account_Balance_Amt ELSE 0 END)	

+ SUM(CASE WHEN PEM.Financial_Class_Code = 15 AND ((CAST ((PEM.Discharge_Date (FORMAT 'YYYYMM')) AS CHAR(6)) > PEM.RPTG_PERIOD)	OR 
( PEM.Account_Status_Code = 'UB' AND PEM.Discharge_Date IS NOT NULL AND CAST(( PEM.Discharge_Date (FORMAT 'YYYYMM')) AS CHAR(6)) <= PEM.Rptg_Period AND 
PEM.Total_Account_Balance_Amt <> 0)) THEN PEM.Total_Account_Balance_Amt ELSE 0 END) 	  

+ SUM(CASE WHEN ((PEM.Account_Status_Code = 'UB' AND SUBSTR(PEM.Patient_Type_Code,2,1) <> 'P' AND PEM.Discharge_Date IS NULL 
AND PEM.Total_Account_Balance_Amt <> 0) OR ( PEM.Account_Status_Code = 'UB' AND PEM.Unbill_Pre_Admit_Ind = 'Y')) AND 
PEM.Financial_Class_Code = 15	THEN PEM.Total_Account_Balance_Amt ELSE 0 
	 END ) 
	 
+ SUM(
	 		CASE WHEN CAST((PEM.Discharge_Date (FORMAT 'YYYYMM')) AS CHAR(6)) <= PEM.RPTG_PERIOD
	 			AND PEM.Account_Status_Code NOT IN ('UB', 'BD') AND PEM.Payor_Balance_Amt_Ins1 > 0 AND PEM.Financial_Class_Code_Ins1 = 15	
	 			THEN PEM.Payor_Balance_Amt_Ins1 ELSE 0 END) + 
	 		SUM(CASE WHEN CAST((PEM.Discharge_Date (FORMAT 'YYYYMM')) AS CHAR(6)) <= PEM.RPTG_PERIOD	
	 			AND PEM.Account_Status_Code NOT IN ('UB', 'BD') AND PEM.Payor_Balance_Amt_Ins2 > 0 AND PEM.Financial_Class_Code_Ins2 = 15	
	 			THEN PEM.Payor_Balance_Amt_Ins2 ELSE 0 END) +
	 		SUM(CASE WHEN CAST((PEM.Discharge_Date (FORMAT 'YYYYMM')) AS CHAR(6)) <= PEM.RPTG_PERIOD	
	 			AND PEM.Account_Status_Code NOT IN ('UB', 'BD') AND PEM.Payor_Balance_Amt_Ins3 > 0 AND PEM.Financial_Class_Code_Ins3 = 15	
			THEN PEM.Payor_Balance_Amt_Ins3 ELSE 0 END
	) 
	+ SUM(CASE	WHEN CAST((PEM.Discharge_Date (FORMAT 'YYYYMM')) AS CHAR(6)) <= PEM.RPTG_PERIOD	
			AND	PEM.Account_Status_Code NOT IN ('UB', 'BD') AND	PEM.Payor_Balance_Amt_Ins1 > 0 AND PEM.Financial_Class_Code_Ins1 = 99	
			THEN PEM.Payor_Balance_Amt_Ins1 
			ELSE	0 
			END) +
			SUM(CASE	WHEN CAST((PEM.Discharge_Date (FORMAT 'YYYYMM')) AS CHAR(6)) <= PEM.RPTG_PERIOD	
					AND	PEM.Account_Status_Code NOT IN ('UB', 'BD') AND	PEM.Payor_Balance_Amt_Ins2 > 0 AND	PEM.Financial_Class_Code_Ins2 = 99	
					THEN PEM.Payor_Balance_Amt_Ins2 
					ELSE	0 
					END	) +
			SUM(CASE	WHEN CAST((PEM.Discharge_Date (FORMAT 'YYYYMM')) AS CHAR(6)) <= PEM.RPTG_PERIOD	
					AND	PEM.Account_Status_Code NOT IN ('UB', 'BD') 	AND	PEM.Payor_Balance_Amt_Ins3 > 0 	AND	PEM.Financial_Class_Code_Ins3 = 99	
					THEN PEM.Payor_Balance_Amt_Ins3 
					ELSE	0 
					END	) 
- SUM(CASE	WHEN PEM.Account_Status_Code IN ('AR','AX','CA') 	AND	CAST ((PEM.Discharge_Date (FORMAT 'YYYYMM')) AS CHAR(6)) <= PEM.RPTG_PERIOD	
			AND	PEM.Total_Account_Balance_Amt > 0	
			--AND PEM.Collector_Org_Code IN (858,859,860,865,867,703) 
			AND TRIM(PEM.collector_org_type_code) ='S'
			THEN PEM.Total_Account_Balance_Amt 
			ELSE	0 
			END) AS Non_Sec_Self_pay_AR_AMT
	

/*TotUninsuredDiscount*/			
,SUM(CASE WHEN PEM.Account_Status_Code in ('AR','AX','CA', 'UB')	
		AND CAST((PEM.Discharge_Date (FORMAT 'YYYYMM')) AS CHAR(6)) <= RPTG_PERIOD	
		AND PEM.Total_Account_Balance_Amt > 0	
	and  (PEM.IPlan_ID_Ins1 in ('09940','09941','09942','09943','09944', '09945', '09946','09947','09948','09949')  and  PEM.Financial_Class_Code_Ins1='99')---Added 9943 and 9948
THEN PEM.Payor_contract_allow_amt_ins1  + PEM.payor_adjustment_amt_ins1 ELSE 0 END ) + 

SUM(CASE WHEN PEM.Account_Status_Code in ('AR','AX','CA', 'UB')	
		AND CAST((PEM.Discharge_Date (FORMAT 'YYYYMM')) AS CHAR(6)) <= RPTG_PERIOD	
		AND PEM.Total_Account_Balance_Amt > 0	
	and  (PEM.IPlan_ID_Ins2 in ('09940','09941','09942','09943','09944', '09945', '09946','09947','09948','09949') and  PEM.Financial_Class_Code_Ins2='99') -----Added 9943 and 9948
--and financial_class_code='99'
THEN PEM.Payor_contract_allow_amt_ins2 + PEM.payor_adjustment_amt_ins2 ELSE 0 END) +

SUM(CASE WHEN PEM.Account_Status_Code in ('AR','AX','CA', 'UB')	
		AND CAST((PEM.Discharge_Date (FORMAT 'YYYYMM')) AS CHAR(6)) <= RPTG_PERIOD	
		AND PEM.Total_Account_Balance_Amt > 0	
	and  (PEM.IPlan_ID_Ins3 in ('09940','09941','09942','09943','09944', '09945', '09946','09947','09948','09949') and PEM.Financial_Class_Code_Ins3='99') -----Added 9943 and 9948
--and financial_class_code='99'
THEN PEM.Payor_contract_allow_amt_ins3 + PEM.payor_adjustment_amt_ins3 ELSE 0 END) 

 AS TotUninsuredDiscount
	
/*TotalCharityExpansionDiscount*/
,SUM(CASE WHEN PEM.Account_Status_Code in ('AR','AX','CA', 'UB')	
AND CAST((PEM.Discharge_Date (FORMAT 'YYYYMM')) AS CHAR(6)) <= RPTG_PERIOD	
AND PEM.Total_Account_Balance_Amt > 0	
and (PEM.IPlan_ID_Ins1 in ('09927')   and  Financial_Class_Code_Ins1='15')
then PEM.Payor_contract_allow_amt_ins1 +  PEM.payor_adjustment_amt_ins1 else 0 end) + 

SUM(CASE WHEN PEM.Account_Status_Code in ('AR','AX','CA', 'UB')	
AND CAST((PEM.Discharge_Date (FORMAT 'YYYYMM')) AS CHAR(6)) <= RPTG_PERIOD	
AND PEM.Total_Account_Balance_Amt > 0	
AND (PEM.IPlan_ID_Ins2 in ('09927') and  Financial_Class_Code_Ins2='15') 
then PEM.Payor_contract_allow_amt_ins2 +  PEM.payor_adjustment_amt_ins2 else 0 end)  +

SUM(CASE WHEN PEM.Account_Status_Code in ('AR','AX','CA', 'UB')	
AND CAST((PEM.Discharge_Date (FORMAT 'YYYYMM')) AS CHAR(6)) <= RPTG_PERIOD	
AND PEM.Total_Account_Balance_Amt > 0	
and (PEM.IPlan_ID_Ins3 in ('09927') and Financial_Class_Code_Ins3='15') 
then PEM.Payor_contract_allow_amt_ins3 +  PEM.payor_adjustment_amt_ins3 else 0 end)
AS TotalCharityExpansionDiscount

		
/*SecondaryAgencyCharityExpansionDiscount*/
,SUM(CASE WHEN PEM.Account_Status_Code in ('AR','AX','CA', 'UB')	
AND CAST((PEM.Discharge_Date (FORMAT 'YYYYMM')) AS CHAR(6)) <= PEM.RPTG_PERIOD	
AND PEM.Total_Account_Balance_Amt > 0	
and (PEM.IPlan_ID_Ins1 in ('09927')   and  Financial_Class_Code_Ins1=15)
and TRIM(PEM.collector_org_type_code)='S'
then PEM.Payor_contract_allow_amt_ins1 +  PEM.payor_adjustment_amt_ins1 else 0 end) +

SUM(CASE WHEN PEM.Account_Status_Code in ('AR','AX','CA', 'UB')	
AND CAST((PEM.Discharge_Date (FORMAT 'YYYYMM')) AS CHAR(6)) <= PEM.RPTG_PERIOD	
AND PEM.Total_Account_Balance_Amt > 0	
AND (PEM.IPlan_ID_Ins2 in ('09927') and  Financial_Class_Code_Ins2=15) 
and TRIM(PEM.collector_org_type_code)='S'
then PEM.Payor_contract_allow_amt_ins2 +  PEM.payor_adjustment_amt_ins2 else 0 end) +

SUM(CASE WHEN PEM.Account_Status_Code in ('AR','AX','CA', 'UB')	
AND CAST((PEM.Discharge_Date (FORMAT 'YYYYMM')) AS CHAR(6)) <= PEM.RPTG_PERIOD	
AND PEM.Total_Account_Balance_Amt > 0	
and (PEM.IPlan_ID_Ins3 in ('09927') and Financial_Class_Code_Ins3=15) 
and TRIM(PEM.collector_org_type_code)='S'
then PEM.Payor_contract_allow_amt_ins3 +  PEM.payor_adjustment_amt_ins3  else 0 end) 

 AS SecondaryAgyCharityExpDiscount
	
/*SecondaryAgyUninsuredDisc*/

,SUM(CASE WHEN PEM.Account_Status_Code in ('AR','AX','CA', 'UB')	
		AND CAST((PEM.Discharge_Date (FORMAT 'YYYYMM')) AS CHAR(6)) <= PEM.RPTG_PERIOD	
		AND PEM.Total_Account_Balance_Amt > 0	
	and  (PEM.IPlan_ID_Ins1 in ('09940','09941','09942','09943','09944', '09945', '09946','09947','09948','09949')  and  Financial_Class_Code_Ins1=99) --Added 9943 and 9948
and TRIM(PEM.collector_org_type_code)='S'
THEN PEM.Payor_contract_allow_amt_ins1 + PEM.payor_adjustment_amt_ins1 ELSE 0 END) +

SUM(CASE WHEN PEM.Account_Status_Code in ('AR','AX','CA', 'UB')	
		AND CAST((PEM.Discharge_Date (FORMAT 'YYYYMM')) AS CHAR(6)) <= RPTG_PERIOD	
		AND PEM.Total_Account_Balance_Amt > 0	
	and  (PEM.IPlan_ID_Ins2 in ('09940','09941','09942','09943','09944', '09945', '09946','09947','09948','09949') and  Financial_Class_Code_Ins2=99) --Added 9943 and 9948
and TRIM(PEM.collector_org_type_code)='S'
THEN PEM.Payor_contract_allow_amt_ins2 + PEM.payor_adjustment_amt_ins2 ELSE 0 END) +

SUM(CASE WHEN PEM.Account_Status_Code in ('AR','AX','CA', 'UB')	
		AND CAST((PEM.Discharge_Date (FORMAT 'YYYYMM')) AS CHAR(6)) <= RPTG_PERIOD	
		AND PEM.Total_Account_Balance_Amt > 0	
	and  (PEM.IPlan_ID_Ins3 in ('09940','09941','09942','09943','09944', '09945', '09946','09947','09948','09949') and Financial_Class_Code_Ins3=99) --Added 9943 and 9948
and TRIM(PEM.collector_org_type_code)='S'
THEN PEM.Payor_contract_allow_amt_ins3 + PEM.payor_adjustment_amt_ins3 ELSE 0 END) 

 AS SecondaryAgyUninsuredDisc
 
 ,SUM (case when PEM.TOTAL_ACCOUNT_BALANCE_AMT > 0 then coalesce(AR_PLP.AR_TRANSACTION_AMT,0) else 0 end ) AS PLP_AMT
,SUM( case when  PEM.TOTAL_ACCOUNT_BALANCE_AMT >0  then coalesce(AR_PLP_SEC.AR_TRANSACTION_AMT,0) else  0 end)  AS SEC_PLP_AMT
,Coalesce(PLP_AMT,0)-Coalesce(Sec_PLP_AMT,0) as Pat_Liability_Protection_Discount 


,(TotUninsuredDiscount + TotalCharityExpansionDiscount- SecondaryAgyUninsuredDisc - SecondaryAgyCharityExpDiscount+Pat_Liability_Protection_Discount) as	Non_Sec_Uninsured_discount
, SUM(0.000) AS Transaction_Amt
FROM	Edwpf_Base_Views.Pass_EOM PEM

JOIN Edwpf_Base_Views.Facility_Dimension FD 
ON	FD.Unit_Num = PEM.Unit_Num

LEFT OUTER JOIN 
(

select coid, pat_acct_num, sum(ar_transaction_amt) ar_transaction_amt from 
(
SELECT  a.coid, a.pat_acct_num, SUM(ar_transaction_amt ) AS ar_transaction_amt
FROM edwpf_base_views.ar_transaction_MV a
JOIN 
( SELECT * FROM edwpf_base_views.pass_eom WHERE TOTAL_ACCOUNT_BALANCE_AMT > 0 and rptg_period=CAST( ( ADD_MONTHS(CURRENT_DATE , -1) (FORMAT 'YYYYMM') ) AS CHAR(6) ) 
)  pa
ON pa.coid = a.coid
AND pa.pat_acct_num = a.pat_Acct_num
WHERE 
transaction_type_code IN (4,5) AND 
transaction_code IN (784984,784985) AND iplan_id IN ( 9929, 0) 
GROUP BY 1,2
UNION ALL
SELECT  a.coid,a.pat_acct_num,  SUM(ar_transaction_amt ) AS ar_transaction_amt
FROM edwpf_base_views.ar_transaction_MV a
JOIN 
( SELECT * FROM edwpf_base_views.pass_eom WHERE TOTAL_ACCOUNT_BALANCE_AMT > 0 and rptg_period=CAST( ( ADD_MONTHS(CURRENT_DATE , -1) (FORMAT 'YYYYMM') ) AS CHAR(6) ) 
)  pa
ON pa.coid = a.coid
AND pa.pat_acct_num = a.pat_Acct_num
WHERE  
transaction_type_code IN (4,5) AND 
transaction_code IN (999999) AND iplan_id = 9929 
GROUP BY 1,2 
UNION ALL
SELECT  a.coid,a.pat_acct_num,  SUM(ar_transaction_amt ) AS ar_transaction_amt
FROM edwpf_base_views.ar_transaction_MV a
JOIN 
( SELECT * FROM edwpf_base_views.pass_eom WHERE   TOTAL_ACCOUNT_BALANCE_AMT > 0 and rptg_period=CAST( ( ADD_MONTHS(CURRENT_DATE , -1) (FORMAT 'YYYYMM') ) AS CHAR(6) ) 
)  pa
ON pa.coid = a.coid
AND pa.pat_acct_num = a.pat_Acct_num
WHERE  
transaction_type_code IN (4,5) 
AND IPLAN_ID =9929 
AND   transaction_code IN(920972,920970,920980,920971, 920981, 920982)  
GROUP BY 1,2 
) k
GROUP BY 1,2 
) ar_plp
ON    ar_plp.coid = PEM.coid
AND ar_plp.pat_Acct_num = PEM.pat_Acct_num

LEFT OUTER JOIN 
(
select coid, pat_acct_num, sum(ar_transaction_amt) ar_transaction_amt from 
(
SELECT  a.coid, a.pat_acct_num, SUM(ar_transaction_amt ) AS ar_transaction_amt
FROM edwpf_base_views.ar_transaction_MV a
JOIN 
( SELECT * FROM edwpf_base_views.pass_eom WHERE rptg_period = CAST( ( ADD_MONTHS(CURRENT_DATE , -1) (FORMAT 'YYYYMM') ) AS CHAR(6) ) AND 
COLLECTOR_ORG_TYPE_CODE='S' and   TOTAL_ACCOUNT_BALANCE_AMT > 0)  p
ON p.coid = a.coid
AND p.pat_acct_num = a.pat_Acct_num
WHERE 
transaction_type_code IN (4,5) AND 
transaction_code IN (784984,784985) AND iplan_id IN ( 9929, 0) 
GROUP BY 1,2
UNION ALL
SELECT  a.coid,a.pat_acct_num,  SUM(ar_transaction_amt ) AS ar_transaction_amt
FROM edwpf_base_views.ar_transaction_MV a
JOIN ( SELECT * FROM edwpf_base_views.pass_eom WHERE rptg_period = CAST( ( ADD_MONTHS(CURRENT_DATE , -1) (FORMAT 'YYYYMM') ) AS CHAR(6) ) AND COLLECTOR_ORG_TYPE_CODE='S' and   TOTAL_ACCOUNT_BALANCE_AMT > 0)  p
ON p.coid = a.coid
AND p.pat_acct_num = a.pat_Acct_num
WHERE  
transaction_type_code IN (4,5) AND 
transaction_code IN (999999) AND iplan_id = 9929 
GROUP BY 1,2 
UNION ALL
SELECT  a.coid,a.pat_acct_num,  SUM(ar_transaction_amt ) AS ar_transaction_amt
FROM edwpf_base_views.ar_transaction_MV a
JOIN ( SELECT * FROM edwpf_base_views.pass_eom WHERE rptg_period=CAST( ( ADD_MONTHS(CURRENT_DATE , -1) (FORMAT 'YYYYMM') ) AS CHAR(6) ) 
AND COLLECTOR_ORG_TYPE_CODE='S' and   TOTAL_ACCOUNT_BALANCE_AMT > 0)  p
ON p.coid = a.coid
AND p.pat_acct_num = a.pat_Acct_num
WHERE  
transaction_type_code IN (4,5) 
AND       IPLAN_ID =9929 
AND       transaction_code IN(920972,920970,920980,920971, 920981, 920982)  
GROUP BY 1,2 
) k1
GROUP BY 1,2 
) ar_plp_sec
ON    ar_plp_sec.coid = PEM.coid
AND ar_plp_sec.pat_Acct_num = PEM.pat_Acct_num



WHERE	
PEM.Rptg_Period =   CAST( ( EOM_END (FORMAT 'YYYYMM') ) AS CHAR(6) ) 
AND	LEFT(PEM. Patient_Type_Code,1) IN ('O', 'E', 'S', 'I') 
AND FD.summary_7_member_ind = 'Y'  AND FD.Company_Code = 'H'
GROUP BY 1
)IN_QUERY GROUP BY 1
union
SELECT	
	 CAST ( ( ( (CURRENT_DATE) (FORMAT 'YYYY-MM') )  || '-01'  )    AS DATE ) - 1 AS EOM_END
, SUM(0) AS Non_Sec_Self_pay_AR_AMT
, SUM(0) AS Non_Sec_Uninsured_discount
 , 	SUM (CASE WHEN AR_Transaction_Comment_Text = 'Bad Debt' AND Transaction_Type_Code = '3'
	 AND 	(FP.Account_Status_Code <> 'IN'  	OR	FP.Account_Status_Code IS NULL)
	THEN AR_Transaction_Amt
	WHEN Transaction_Type_Code IN ('4','5') AND ar.Debit_GL_Dept_num ='110' AND ar.Debit_gl_sub_account_num ='090' THEN 	AR_Transaction_Amt ELSE 0 END)
AS Transaction_amt					
FROM	Edwpf_Base_Views.AR_Transaction_MV AR JOIN Edwpf_Base_Views.Fact_Patient FP ON	AR.Patient_DW_ID = FP.Patient_DW_ID   
         							
JOIN Edwpf_Base_Views.Facility_Dimension FD ON	FD.Unit_Num = FP.Unit_Num 

WHERE((AR_Transaction_Effective_Date BETWEEN  CAST ( ( ( (EOM_END) (FORMAT 'YYYY-MM') ) || '-01' ) AS DATE) 	AND EOM_END
			AND	 AR_Transaction_Enter_Date BETWEEN   CAST ( ( ( (EOM_END) (FORMAT 'YYYY-MM') ) || '-01' )  AS DATE) 	AND	 EOM_END + 4 )
		OR		( AR_Transaction_Enter_Date BETWEEN  CAST ( ( ( (EOM_END) (FORMAT 'YYYY-MM') ) || '-05' ) AS DATE)	AND EOM_END  + 4          
			AND	AR_Transaction_Effective_Date < EOM_END + 1 )
		)	
AND	Transaction_Type_Code IN ( '3', '4', '5') 
AND ( Patient_Type_Code_Pos1 IN ('O', 'E', 'S', 'I','N')   OR Patient_Type_Code_Pos1 IS NULL)
AND FD.summary_7_member_ind = 'Y'  AND FD.Company_Code = 'H'
GROUP	BY 1)  k
"

#  @@END DMEXPRESS_EXPORTED_VARIABLES
#  End of Exported variables section
#  
