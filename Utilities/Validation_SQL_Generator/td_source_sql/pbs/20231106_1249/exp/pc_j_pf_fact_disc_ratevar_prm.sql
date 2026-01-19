Select  'PBMPC310'||','||
COALESCE(cast(SUM(X.EOR_Gross_Reimbursement_Amt) as varchar(20)),'0')||','||
COALESCE(cast(SUM(X.EOR_Contractual_Allowance_Amt) as varchar(20)),'0')||','||
COALESCE(cast(SUM(X.EOR_Insurance_Payment_Amt)  as varchar(20)),'0')||',' as SOURCE_STRING
From (
SELECT 
       C.Company_Code
     , C.Coid
     , CAST(C.Patient_DW_ID AS VARCHAR(20)) AS Patient_DW_ID
     , CAST(C.Payor_DW_ID AS VARCHAR(20)) AS Payor_DW_ID
     , C.Log_ID
     , C.EOR_Log_Date
     , C.Log_Sequence_Num
     , CASE WHEN D.Final_Bill_Date = '0001-01-01' 
            THEN D.AR_Bill_Thru_Date 
            ELSE D.Final_Bill_Date 
        END AS FinalBillDate
     , AD.Discharge_Date
     , D.Financial_Class_Code
     , FF.Unit_Num
     , CASE WHEN (SUBSTR(APT.Admission_Patient_Type_Code,1,1) = 'I' 
             AND FRL.IP_Log_FORMAT_Code = 'O') THEN '0124'
            WHEN (SUBSTR(APT.Admission_Patient_Type_Code,1,1) = 'I'
             AND FRL.IP_Log_FORMAT_Code = 'M') THEN '0125'
            WHEN (SUBSTR(APT.Admission_Patient_Type_Code,1,1) <> 'I'
             AND FRL.OP_Log_FORMAT_Code = 'O') THEN '0124'
            WHEN (SUBSTR(APT.Admission_Patient_Type_Code,1,1) <> 'I'
             AND FRL.OP_Log_FORMAT_Code = 'M') THEN '0125'
            WHEN D.Source_System_Code = 'N'
             AND D.Financial_Class_Code IN (1,2,3,6,9)
            THEN '0125'
            WHEN D.Source_System_Code = 'N'
            THEN '0124'
            ELSE '0000'
        END AS DataSourceCode
     , APT.Admission_Patient_Type_Code
     , CAST(sum(D.EOR_Gross_Reimbursement_Amt) AS VARCHAR(20)) AS EOR_Gross_Reimbursement_Amt
     , CAST(sum(D.EOR_Contractual_Allowance_Amt) AS VARCHAR(20)) AS EOR_Contractual_Allowance_Amt
     , CAST(sum(D.EOR_Insurance_Payment_Amt) AS VARCHAR(20)) AS EOR_Insurance_Payment_Amt
     , D.AR_Bill_Thru_Date
     , D.Source_System_Code
     ,max(coalesce(current_date-extract(day from current_date)-AD.Discharge_Date,0)) as Disch_days
  FROM Edwpf_Base_Views.Explanation_Of_Reimbursement D
  JOIN (SELECT K.Company_Code
             , K.Coid
             , K.Patient_DW_ID
             , K.Payor_DW_ID
             , K.Log_ID
             , K.AR_Bill_Thru_Date
             , K.Iplan_Insurance_Order_Num
             , K.Final_Bill_Date
             , K.EOR_Log_Date
             , K.Log_Sequence_Num
             , MIN(K.Eff_From_Date) AS Eff_From_Date
          FROM Edwpf_Base_Views.Explanation_Of_Reimbursement K                                        
          JOIN (SELECT H.Company_Code
                     , H.Coid
                     , H.Patient_DW_ID
                     , H.Payor_DW_ID
                     , H.Log_ID
                     , H.AR_Bill_Thru_Date
                     , H.Iplan_Insurance_Order_Num
                     , H.Final_Bill_Date
                     , H.EOR_Log_Date
                     , MIN(H.Log_Sequence_Num) AS Log_Sequence_Num
                  FROM Edwpf_Base_Views.Explanation_Of_Reimbursement H                                        
                  JOIN (SELECT A.Company_Code 
                             , A.Coid
                             , A.Patient_DW_ID
                             , A.Payor_DW_ID
                             , A.Log_ID
                             , A.AR_Bill_Thru_Date
                             , A.Iplan_Insurance_Order_Num
                             , A.Final_Bill_Date
                             , MIN(A.EOR_Log_Date) AS EOR_Log_Date
                         FROM Edwpf_Base_Views.Explanation_Of_Reimbursement A     
                         JOIN (SELECT B.Company_Code
                                    , B.Coid
                                    , B.Patient_DW_ID
                                    , B.Payor_DW_ID
                                    , B.Log_ID
                                    , B.AR_Bill_Thru_Date
                                    , B.Final_Bill_Date
                                    , B.Iplan_Insurance_Order_Num
                                 FROM Edwpf_Base_Views.Explanation_Of_Reimbursement B
                                WHERE B.EOR_Reversal_Code = ' '
                                  AND B.Iplan_Insurance_Order_Num = 1
                                GROUP by 1,2,3,4,5,6,7,8
                               ) Z 
                           ON A.Company_Code = Z.Company_Code 
                          AND A.Coid = Z.Coid 
                          AND A.Patient_DW_ID = Z.Patient_DW_ID
                          AND A.Payor_DW_ID = Z.Payor_DW_ID                         
                          AND A.Log_ID = Z.Log_ID 
                          AND A.AR_Bill_Thru_Date = Z.AR_Bill_Thru_Date
                          AND A.Final_Bill_Date = Z.Final_Bill_Date
                          AND A.Iplan_Insurance_Order_Num = Z.Iplan_Insurance_Order_Num
                         GROUP BY 1,2,3,4,5,6,7,8
                       ) G 
                    ON H.Company_Code = G.Company_Code 
                   AND H.Coid = G.Coid 
                   AND H.Patient_DW_ID = G.Patient_DW_ID
                   AND H.Payor_DW_ID = G.Payor_DW_ID                                        
                   AND H.Log_ID = G.Log_ID 
                   AND H.AR_Bill_Thru_Date = G.AR_Bill_Thru_Date
                   AND H.Final_Bill_Date = G.Final_Bill_Date
                   AND H.Iplan_Insurance_Order_Num = G.Iplan_Insurance_Order_Num
                   AND H.EOR_Log_Date = G.EOR_Log_Date
                 GROUP BY 1,2,3,4,5,6,7,8,9
               ) J 
            ON K.Company_Code = J.Company_Code 
           AND K.Coid = J.Coid 
           AND K.Patient_DW_ID = J.Patient_DW_ID
           AND K.Payor_DW_ID = J.Payor_DW_ID                              
           AND K.Log_ID = J.Log_ID 
           AND K.AR_Bill_Thru_Date = J.AR_Bill_Thru_Date
           AND K.Final_Bill_Date = J.Final_Bill_Date
           AND K.Iplan_Insurance_Order_Num = J.Iplan_Insurance_Order_Num
           AND K.EOR_Log_Date = J.EOR_Log_Date
           AND K.Log_Sequence_Num = J.Log_Sequence_Num
         GROUP BY 1,2,3,4,5,6,7,8,9,10
        ) C 
     ON D.Company_Code = C.Company_Code 
    AND D.Coid = C.Coid 
    AND D.Patient_DW_ID = C.Patient_DW_ID
    AND D.Payor_DW_ID = C.Payor_DW_ID 
    AND D.Log_ID = C.Log_ID 
    AND D.Final_Bill_Date = C.Final_Bill_Date
    AND D.AR_Bill_Thru_Date = C.AR_Bill_Thru_Date 
    AND D.EOR_Log_Date = C.EOR_Log_Date
    AND D.Log_Sequence_Num = C.Log_Sequence_Num 
    AND D.Eff_From_Date = C.Eff_From_Date
    AND D.Iplan_Insurance_Order_Num = C.Iplan_Insurance_Order_Num
   LEFT JOIN EDWPF_Base_Views.Admission_Discharge AD
     ON AD.Patient_DW_Id = D.Patient_DW_Id
   LEFT JOIN EDWFS_Base_Views.FACILITY FF
     ON FF.Coid = D.Coid
    AND FF.Company_Code = D.Company_Code
   JOIN (Select PT.Patient_Dw_Id
              , MAX(PT.Eff_From_Date) AS Eff_From_Date
           FROM EDWPF_Base_Views.Admission_Patient_Type PT 
          GROUP BY 1  ) ATP 
     ON ATP.Patient_DW_Id = D.Patient_DW_Id
   JOIN EDWPF_Base_Views.Admission_Patient_Type APT
     ON APT.Patient_DW_Id = D.Patient_DW_Id
    AND APT.Eff_From_Date = ATP.Eff_From_Date           
   LEFT JOIN EDWPF_Base_Views.Facility_Reimbursement_Log FRL 
     ON D.Company_Code = FRL.Company_Code
    AND D.Coid = FRL.Coid
    AND D.Log_ID = FRL.Log_ID
    AND FRL.Log_Reports_Ind = 'Y'
    AND FRL.Inactive_Date > (CAST((CAST(((CURRENT_DATE) (FORMAT'yyyy-mm')) AS CHAR(7)) || '-01') AS DATE) -1)
    AND D.Source_System_Code = 'P'
  WHERE D.Log_ID <> 'MSP'
    AND FinalBillDate BETWEEN CAST((CAST((ADD_MONTHS(CURRENT_DATE, -3)(FORMAT'yyyy-mm'))  AS CHAR(7)) || '-01') AS DATE)  
                          AND (CAST((CAST((ADD_MONTHS(CURRENT_DATE, -1) (FORMAT'yyyy-mm')) AS CHAR(7)) || '-01') AS DATE) -1)
    AND DataSourceCode <> '0000'
    AND D.Coid not in (select coid from edwpbs.parallon_client_detail where Company_code='CHP')
	
	Group by 
C.Company_Code, C.Coid, c.Patient_DW_ID, c.Payor_DW_ID, C.Log_ID, C.EOR_Log_Date, C.Log_Sequence_Num, FinalBillDate, AD.Discharge_Date
     , D.Financial_Class_Code, FF.Unit_Num, DataSourceCode, APT.Admission_Patient_Type_Code, D.AR_Bill_Thru_Date, D.Source_System_Code	)x
