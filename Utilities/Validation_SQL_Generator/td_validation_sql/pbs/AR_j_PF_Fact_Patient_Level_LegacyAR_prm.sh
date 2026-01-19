
#  @@START DMEXPRESS_EXPORTED_VARIABLES

export AC_EXP_TOLERANCE_PERCENT=0
export AC_EXP_TOLERANCE_AMT=0
export JOBNAME='PBMAR100'
export AC_ACT_SQL_STATEMENT="Locking table  edwpf.fact_rcom_ar_patient_level for access
SELECT 
'PBMAR100' || ',' ||
cast(zeroifnull(SUM(ARP.Payor_Payment_Amt)) as varchar(20))  || ',' ||
cast(zeroifnull(SUM(ARP.AR_Patient_Amt)) as varchar(20))  || ',' ||
cast(zeroifnull(SUM(ARP.AR_Insurance_Amt)) as varchar(20))  || ',' ||
cast(zeroifnull(SUM(ARP.Gross_Charge_Amt)) as varchar(20))  || ',' ||
cast(zeroifnull(SUM(ARP.Payor_Contractual_Amt)) as varchar(20))  || ',' AS SOURCE_STRING

FROM
 edwpf.fact_rcom_ar_patient_level ARP
Where 
ARP.Date_Sid = cast((add_months(current_date, -1)(Format 'yyyymm')) as char(6))
AND ARP.Patient_SID <>0 and Source_Sid=9
AND ARP.Coid in (select co_id from edwpbs_staging.pass_current group by 1)
"

export AC_EXP_SQL_STATEMENT="locking table edwpbs_staging.pass_current for access
SELECT 
'PBMAR100'  || ',' ||
Trim(SUM(ZEROIFNULL(EOM_Pymt_Rcvd)))  || ',' ||
Trim(SUM(CASE WHEN Ins_Order_Num = 0 AND TRIM(BOTH FROM Acct_Stat)  <>  'UB'
                    THEN    ZEROIFNULL(EOM_Pat_Bal)
                     WHEN Ins_Order_Num = 0 AND TRIM(BOTH FROM Acct_Stat)  =  'UB'
                     THEN ZEROIFNULL(EOM_Tot_Acct_Bal)
                     WHEN Ins_Order_Num = 1 
                                                THEN (CASE WHEN (CAST(INS_PLAN1 AS INTEGER) = 9940 OR CAST(INS_PLAN1 AS INTEGER) = 9941 OR CAST(INS_PLAN1 AS INTEGER) = 9942 OR CAST(INS_PLAN1 AS INTEGER) = 9943 OR CAST(INS_PLAN1 AS INTEGER) = 9944 OR CAST(INS_PLAN1 AS INTEGER) = 9945 OR CAST(INS_PLAN1 AS INTEGER) = 9946 OR CAST(INS_PLAN1 AS INTEGER) = 9947 OR CAST(INS_PLAN1 AS INTEGER) = 9948 OR CAST(INS_PLAN1 AS INTEGER) = 9949  
                                                                                                OR (CAST(INS_PLAN1 AS INTEGER)= 9975 AND (CAST(CO_ID AS INTEGER) = 2348 OR CAST(CO_ID AS INTEGER) = 2560)) OR
                                                                                                (CAST(INS_PLAN1 AS INTEGER) = 95 AND (CAST(CO_ID AS INTEGER) = 2531 OR CAST(CO_ID AS INTEGER) = 1574 OR CAST(CO_ID AS INTEGER) = 1578  
                                                                                                OR CAST(CO_ID AS INTEGER) = 1588 OR CAST(CO_ID AS INTEGER) = 6676 OR CAST(CO_ID AS INTEGER) = 6678 OR CAST(CO_ID AS INTEGER) = 6679 
                                                                                                OR CAST(CO_ID AS INTEGER) = 196 OR CAST(CO_ID AS INTEGER) = 6240 OR CAST(CO_ID AS INTEGER) = 8224)))
                                                                                    THEN 0
                                                                                    ELSE ZEROIFNULL(EOM_Pat_Bal)
                                                                        END)
                        ELSE 0
           END)  )  || ',' ||
Trim(SUM(CASE WHEN Ins_Order_Num <> 0
                    THEN ZEROIFNULL(EOM_Ins_Bal)
                     ELSE 0.00
            END)  )   || ',' ||
Trim(SUM(CASE WHEN Ins_Order_Num = 0 AND CAST(CAST( CAST (DISCH_DT AS DATE FORMAT 'YYYYMM') AS CHAR(6))  AS INTEGER) = CAST(RPTG_Period AS INTEGER)
                   THEN CAST(ZEROIFNULL(EOM_Total_Chgs) AS DECIMAL (18,3)) 
                   ELSE 0.000
            END)   )  || ',' ||
Trim(SUM(ZEROIFNULL(EOM_All_Rcvd))  )  || ','  AS SOURCE_STRING

FROM (

SELECT 
PC.RPTG_PERIOD, 
PC.ACCT_STAT,
PC.INS_PLAN1,
PC.CO_ID,
CAST(PC.DISCH_DT AS DATE FORMAT 'YYYY-MM-DD'),
CASE WHEN CAST(PC.INS_PLAN1 AS INTEGER)  <> 0
           THEN CAST(PC.EOM_INS_BAL1 AS DECIMAL (18,3)) 
            ELSE 0.000
    END,
CAST(PC.EOM_INS_PYMT_RCVD1 AS DECIMAL (18,3)) ,
CAST(PC.EOM_INS_ALL_RCVD1 AS DECIMAL (18,3)) , 
CAST(PC.EOM_PAT_BAL AS DECIMAL (18,3)) ,
PC.EOM_TOTAL_CHGS, 
CAST(PC.EOM_TOT_ACCT_BAL AS DECIMAL (18,3)) ,
1 
FROM
edwpbs_staging.PASS_Current PC,
edwpf_views.facility_dimension FD
WHERE
PC.CO_ID=FD.UNIT_NUM
and CAST(PC.INS_PLAN1 as INTEGER) != 0
AND PC.PATIENT_DW_ID IS NOT NULL
and pc.disch_dt is not null
and (FD.company_code='H' OR substr(Trim(FD.Company_Code_Operations),1,1) = 'Y')
UNION ALL
SELECT 
PC.RPTG_PERIOD, 
PC.ACCT_STAT, 
'0',
PC.CO_ID,
CAST(PC.DISCH_DT AS DATE FORMAT 'YYYY-MM-DD'),
CASE WHEN CAST(PC.INS_PLAN2 AS INTEGER)  <> 0
           THEN CAST(PC.EOM_INS_BAL2 AS DECIMAL (18,3)) 
            ELSE 0.000
    END,
CAST(PC.EOM_INS_PYMT_RCVD2 AS DECIMAL (18,3)) ,
CAST(PC.EOM_INS_ALL_RCVD2 AS DECIMAL (18,3)) , 
CAST(PC.EOM_PAT_BAL AS DECIMAL (18,3)) ,
PC.EOM_TOTAL_CHGS, 
CAST(PC.EOM_TOT_ACCT_BAL AS DECIMAL (18,3)) ,
2
FROM
edwpbs_staging.PASS_Current PC,
edwpf_views.facility_dimension FD

WHERE
PC.CO_ID=FD.UNIT_NUM
and CAST(PC.INS_PLAN2 as INTEGER) != 0
AND PC.PATIENT_DW_ID IS NOT NULL
and pc.disch_dt is not null
and (FD.company_code='H' OR substr(Trim(FD.Company_Code_Operations),1,1) = 'Y')
UNION ALL
SELECT 
PC.RPTG_PERIOD, 
PC.ACCT_STAT, 
'0',
PC.CO_ID,
CAST(PC.DISCH_DT AS DATE FORMAT 'YYYY-MM-DD'), 
CASE WHEN CAST(PC.INS_PLAN3 AS INTEGER)  <> 0
           THEN CAST(PC.EOM_INS_BAL3 AS DECIMAL (18,3)) 
            ELSE 0.000
    END,
CAST(PC.EOM_INS_PYMT_RCVD3 AS DECIMAL (18,3)) ,
CAST(PC.EOM_INS_ALL_RCVD3 AS DECIMAL (18,3)) , 
CAST(PC.EOM_PAT_BAL AS DECIMAL (18,3)) ,
PC.EOM_TOTAL_CHGS, 
CAST(PC.EOM_TOT_ACCT_BAL AS DECIMAL (18,3)) ,
3
FROM
edwpbs_staging.PASS_Current PC,
edwpf_views.facility_dimension FD
WHERE
PC.CO_ID=FD.UNIT_NUM
and CAST(PC.INS_PLAN3 as INTEGER) != 0
AND PC.PATIENT_DW_ID IS NOT NULL
and pc.disch_dt is not null
and (FD.company_code='H' OR substr(Trim(FD.Company_Code_Operations),1,1) = 'Y')

UNION ALL
SELECT 
PC.RPTG_PERIOD, 
PC.ACCT_STAT, 
'0',
PC.CO_ID,
CAST(PC.DISCH_DT AS DATE FORMAT 'YYYY-MM-DD'), 
CASE WHEN CAST(PC.INS_PLAN1 AS INTEGER)  = 0
           THEN CAST(PC.EOM_PAT_BAL AS DECIMAL (18,3)) 
            ELSE 0.000
    END,
CAST(PC.EOM_PAT_PYMT_RCVD AS DECIMAL (18,3)) , 
CAST(PC.EOM_PAT_ALL_RCVD AS DECIMAL (18,3)) , 
CAST(PC.EOM_PAT_BAL AS DECIMAL (18,3)) ,
PC.EOM_TOTAL_CHGS, 
CAST(PC.EOM_TOT_ACCT_BAL AS DECIMAL (18,3)) ,
0

FROM
edwpbs_staging.PASS_Current PC,
edwpf_views.facility_dimension FD
WHERE
PC.CO_ID=FD.UNIT_NUM
AND PC.PATIENT_DW_ID IS NOT NULL
and pc.disch_dt is not null
and (FD.company_code='H' OR substr(Trim(FD.Company_Code_Operations),1,1) = 'Y')
) SubQ (Rptg_Period, Acct_Stat, INS_PLAN1, CO_ID, Disch_DT, EOM_Ins_Bal, EOM_Pymt_Rcvd, EOM_All_Rcvd, EOM_Pat_Bal, EOM_Total_Chgs, EOM_Tot_Acct_Bal, Ins_Order_Num)"

#  @@END DMEXPRESS_EXPORTED_VARIABLES
#  End of Exported variables section
#  


