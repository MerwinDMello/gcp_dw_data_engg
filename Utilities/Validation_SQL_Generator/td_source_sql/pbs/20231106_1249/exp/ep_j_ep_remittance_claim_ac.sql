SELECT 'J_EP_Remittance_Claim' || ',' || CAST(COUNT(*) AS VARCHAR(20)) || ',' as Source_String from
(
SELECT
A.Claim_GUID AS Claim_GUID
,cast(coalesce(fp.patient_dw_id,'999999' ) as decimal(18,0)) AS Patient_DW_Id
,A.Payment_GUID AS Payment_GUID
,A.Audit_Date AS Audit_Date
,A.Delete_Ind AS Delete_Ind
,coalesce(delete_date,cast('1999-01-01' as date))  AS Delete_Date
,A.Provider_Split_COID AS Coid 
,A.Provider_Split_Unit_Num AS Unit_Num
,A.Company_Code AS Company_Code
,Payor_Patient_Id AS Payor_Patient_Id
,case when remit_account_number> '9999999999' then cast('9999999999' as decimal(10,0)) else remit_account_number end as remit_account_number
,A.Patient_Last_Name AS Patient_Last_Name
,A.Patient_First_Name AS Patient_First_Name
,A.Patient_Middle_Initial AS Patient_Middle_Initial
,A.MPI_Ind AS MPI_Ind
,A.Iplan_Id AS Iplan_Id
,Iplan_id AS EP_Calc_Iplan_Id 
,A.Insurance_Order_Num AS IPLAN_Insurance_Order_Num
,A.Medical_Record_Num AS Medical_Record_Num
,A.Payor_ICN AS Payer_Claim_Control_Number
,A.Service_From_Date AS Stmt_Cover_From_Date
,A.Service_Thru_Date AS Stmt_Cover_To_Date
,To_Date(CASE  
                                                WHEN Length(Received_date)>0 THEN 
                                                                CASE  
                                                                                WHEN Length(StrTok(Received_date, '/', 1)) = 1 AND Length(StrTok(Received_date, '/', 2)) = 1 THEN StrTok(Received_date,'/', 3)||'/0'||StrTok(Received_date, '/', 1)||'/0'||StrTok(Received_date,'/', 2)
                                                                                WHEN Length(StrTok(Received_date, '/', 1)) = 1 THEN StrTok(Received_date,'/', 3)||'/0'||StrTok(Received_date, '/', 1)||'/'||StrTok(Received_date,'/', 2)
                                                                                WHEN Length(StrTok(Received_date, '/', 2)) = 1 THEN StrTok(Received_date,'/', 3)||'/'||StrTok(Received_date, '/', 1)||'/0'||StrTok(Received_date,'/', 2)
                                                                                ELSE StrTok(Received_date,'/', 3)||'/'||StrTok(Received_date, '/', 1)||'/'||StrTok(Received_date,'/', 2)
                                                                END 
                                                ELSE NULL
                                END, 'YYYY/MM/DD') AS Received_date
,A.MPI_Corrected_Discharge_Date AS MPI_Corrected_Discharge_Date
,A.MPI_Calc_Type_Ind AS Patient_Type_Code_Pos1
,A.Financial_Class_Code AS Financial_Class_Code
,A.charge_amt AS Total_Charge_Amt
,A.MPI_Calc_Charge_Amt AS MPI_Calc_Charge_Amt
,A.Payment_Amt AS Payment_Amt
,A.EP_Calc_Denial_Amount AS EP_Calc_Denial_Amount
,A.EP_Calc_Contractual_Adj_Amt AS EP_Calc_Contractual_Adj_Amt
,MPI_Contractual_Adjust_Amount AS MPI_Contractual_Adj_Amt 
,MPI_Calc_Contractual_Adj_Amt AS Corrected_Contractual_Adj_Amt
,Net_Benefit_Amount AS Net_Benefit_Amt
,Covered_Charge_Amt AS Covered_Charge_Amt
,MPI_Calc_Payor_Prev_Paymnt_Amt AS MPI_Calc_Payor_Previous_Payment_Amt
,EP_Calc_NonCovered_Charge_Amt AS EP_Calc_NonCovered_Charge_Amt
,MPI_Calc_Non_Coverd_Charge_Amt AS MPI_Calc_Non_Covered_Charge_Amt
,EP_Calc_Deductible_Amt AS EP_Calc_Deductible_Amt
,Claim_MPI_Deductible_Amount AS MPI_Calc_Deductible_Amt 
,EP_Calc_Blood_Deductible_Amt AS EP_Calc_Blood_Deductible_Amt
,EP_Calc_Coinsurance_Amt AS EP_Calc_Coinsurance_Amt
,Claim_MPI_Coinsurance_Amount AS MPI_Calc_Coinsurance_Amt 
,EP_Calc_Prim_Payor_Payment_Amt AS EP_Calc_Primary_Payor_Payment_Amt
,Patient_Liability_Amount AS Patient_Liability_Amount
,Capital_Amt AS Capital_Amt
,EP_Calc_Discount_Amt AS EP_Calc_Discount_Amt
,Disproportionate_Share_Amt AS Disproportionate_Share_Amt
,DRG_Disproportionate_Share_Amt AS DRG_Disproportionate_Share_Amt
,DRG_Amt AS DRG_Amt
,DRG_Code AS DRG_Code
,Federal_Specific_DRG_Amt AS Federal_Specific_DRG_Amt
,HCPCS_Charge_Amt AS HCPCS_Charge_Amt
,HCPCS_Payment_Amt AS HCPCS_Payment_Amt
,Indirect_Medical_Education_Amt AS Indirect_Medical_Education_Amt
,Indirect_Teaching_Amt AS Indirect_Teaching_Amt
,Interest_Amt AS Interest_Amt
,EP_Calc_Lab_Charge_Amt AS EP_Calc_Lab_Charge_Amt
,EP_Calc_Lab_Payment_Amt AS EP_Calc_Lab_Payment_Amt
,Hospital_Specific_DRG_Amt AS Hospital_Specific_DRG_Amt
,MIA_PPS_Oprtng_Fed_Spc_DRG_Amt AS PPS_Opr_Federal_Specific_DRG_Amt
,MPI_Calc_Non_Billed_Charge_Amt AS MPI_Calc_Non_Billed_Charge_Amt
,MPI_Calc_Net_Benefit_Amt AS MPI_Calc_Net_Benefit_Amt
,EP_Cal_Non_Payble_Prof_Fee_Amt AS EP_Calc_Non_Payable_Professional_Fee_Amt
,Old_Capital_Amt AS Old_Capital_Amt
,Operating_Outlier_Amt AS Operating_Outlier_Amt
,Outlier_Amt AS Outlier_Amt
,Outpatient_Remibursemnt_Rt_Amt AS Outpatient_Reimibursement_Rate_Amt
,EP_Calc_Therapy_Charge_Amt AS EP_Calc_Therapy_Charge_Amt
,EP_Calc_Therapy_Payment_Amt AS EP_Calc_Therapy_Payment_Amt
,PPS_Capital_Outlier_Amt AS PPS_Capital_Outlier_Amt
,Ins_Covered_Day_Cnt AS Ins_Covered_Day_Cnt
,EP_Calc_Covered_Day_Cnt AS EP_Calc_Covered_Day_Cnt
,NonCovered_Day_Cnt AS NonCovered_Day_Cnt
,Cost_Report_Day_Cnt AS Cost_Report_Day_Cnt
,DRG_Weight AS DRG_Weight_Amt
,MPI_Calc_DRG_Code AS MPI_Calc_DRG_Code
,MPI_Calc_DRG_Grouper_Code AS MPI_Calc_DRG_Grouper_Code
,Discharge_Fraction AS Discharge_Fraction_Pct
,MIA_Lifetime_Psych_Day_Cnt AS Lifetime_Psychiatric_Day_Cnt
,Prodr_Code_Contractual_Adj_Amt AS Contractual_Adj_Amt_Procedure_Code
,Procedure_Code_Payment_Amt AS Payment_Amt_Procedure_Code
,Logging_Flag AS Logging_Flag
,Logging_Type AS Logging_Type_Code
,MPI_Calc_Interim_Bill_Ind AS MPI_Calc_Interim_Bill_Ind
,Claim_Status_Code AS Claim_Status_Code
,EP_Calc_Status_Code AS EP_Calc_Status_Code
,EP_Calc_Denial_Code_Ind AS EP_Calc_Denial_Code_Ind
,MPI_Calc_Denial_Code_Ind AS MPI_Calc_Denial_Code_Ind
,Claim_Filing_Indicator_Code AS Type_of_Claim_Code
,Bill_Type_Code AS Type_of_Bill_Code
,EP_Calc_Balanced_Ind AS EP_Calc_Balanced_Ind
,Discount_Applied_Ind AS Discount_Applied_Ind
,EP_Cal_PLB_Ovpym_Recov_Trx_Ind AS EP_Calc_PLB_Opay_Rcvy_Trx_Ind
,EP_Calc_IZ_Denial_Code AS EP_Calc_Internal_Denial_Code 
,Concuity_Acct_Ind AS Concuity_Acct_Ind
,MIA_Switch_Ind AS MIA_Switch_Ind
,MOA_Switch_Ind AS MOA_Switch_Ind
,MIA_PPS_Capital_Exception_Amt AS MIA_PPS_Capital_Exception_Amt
,Casemix_Ind AS Casemix_Ind
,EP_Calc_Denial_Code1 AS EP_Calc_Denial_Code_1
,EP_Calc_Denial_Code2 AS EP_Calc_Denial_Code_2
,EP_Calc_Denial_Code3 AS EP_Calc_Denial_Code_3
,EP_Calc_Denial_Code4 AS EP_Calc_Denial_Code_4
,EP_Calc_Denial_Code5 AS EP_Calc_Denial_Code_5
,EP_Calc_Denial_Code6 AS EP_Calc_Denial_Code_6
,EP_Calc_Denial_Code7 AS EP_Calc_Denial_Code_7
,EP_Calc_Denial_Code8 AS EP_Calc_Denial_Code_8
,EP_Calc_Denial_Code9 AS EP_Calc_Denial_Code_9
,EP_Calc_Denial_Code10 AS EP_Calc_Denial_Code_10
,EP_Calc_Handling_Ind AS EP_Calc_Handling_Ind
,Crossover_Payor_Name AS Crossover_Payor_Name
,EP_Calc_Payor_Category_Code AS EP_Calc_Payor_Category_Code
,Secondary_Payor_Ind AS Secondary_Payor_Flag
,Claim_MPI_Primary_Iplan_Payer AS EP_Calc_Prim_Itnl_Pyr_Code 
,MPI_Secondary_Iplan_Payer AS EP_Calc_Secn_Itnl_Pyr_Code
,Claim_MPI_Tertiary_Iplan_Payer AS EP_Calc_Tert_Itnl_Pyr_Code 
,F.Corrected_Priority_Payor_SID AS Corrected_Priority_Payor_SID 
,E.COB_Carrier_SID AS COB_Carrier_SID 
,Corrected_Subscriber_Last_Name AS Corrected_Subscriber_Last_Name
,Corrected_Subscriber_Frst_Name AS Corrected_Subscriber_First_Name
,Corrected_Subscriber_Mid_Initl AS Corrected_Subscriber_Middle_Name 
,Correctd_Sbscbr_Health_Ins_Num AS Corrected_Subscriber_Health_Ins_Num
,B.Remittance_Subscriber_SID AS Remittance_Subscriber_SID
,C.Remittance_Oth_Subscriber_SID AS Remittance_Oth_Subscriber_SID
,D.Remittance_Rendering_Provider_SID AS Remittance_Rendering_Provider_SID
,Grp_2100_Qualifier1_Code AS Supplemental_Amt_Qlfr_Code_1
,Grp_2100_Supplementl_Info1_Amt AS Supplemental_Amt_1
,Grp_2100_Qualifier2_Code AS Supplemental_Amt_Qlfr_Code_2
,Grp_2100_Supplementl_Info2_Amt AS Supplemental_Amt_2
,Grp_2100_Qualifier3_Code AS Supplemental_Amt_Qlfr_Code_3
,Grp_2100_Supplementl_Info3_Amt AS Supplemental_Amt_3
,Grp_2100_Qualifier4_Code AS Supplemental_Amt_Qlfr_Code_4
,Grp_2100_Supplementl_Info4_Amt AS Supplemental_Amt_4
,Grp_2100_Qualifier5_Code AS Supplemental_Amt_Qlfr_Code_5
,Grp_2100_Supplementl_Info5_Amt AS Supplemental_Amt_5
,Grp_2100_Qualifier6_Code AS Supplemental_Amt_Qlfr_Code_6
,Grp_2100_Supplementl_Info6_Amt AS Supplemental_Amt_6
,Grp_2100_Qualifier7_Code AS Supplemental_Amt_Qlfr_Code_7
,Grp_2100_Supplementl_Info7_Amt AS Supplemental_Amt_7
,Grp_2100_Qualifier8_Code AS Supplemental_Amt_Qlfr_Code_8
,Grp_2100_Supplementl_Info8_Amt AS Supplemental_Amt_8
,Grp_2100_Qualifier9_Code AS Supplemental_Amt_Qlfr_Code_9
,Grp_2100_Supplementl_Info9_Amt AS Supplemental_Amt_9
,Grp_2100_Qualifier10_Code AS Supplemental_Amt_Qlfr_Code_10
,Grp_2100_Supplemntl_Info10_Amt AS Supplemental_Amt_10
,Grp_2100_Qualifier11_Code AS Supplemental_Amt_Qlfr_Code_11
,Grp_2100_Supplemntl_Info11_Amt AS Supplemental_Amt_11
,Grp_2100_Qualifier12_Code AS Supplemental_Amt_Qlfr_Code_12
,Grp_2100_Supplemntl_Info12_Amt AS Supplemental_Amt_12
,Grp_2100_Qualifier13_Code AS Supplemental_Amt_Qlfr_Code_13
,Grp_2100_Supplemntl_Info13_Amt AS Supplemental_Amt_13
,Quantity_Qualifier1_Code AS Supplemental_Qty_Qlfr_Code_1
,Supplemental_Infrmtn1_Quantity AS Supplemental_Qty_1
,Quantity_Qualifier2_Code AS Supplemental_Qty_Qlfr_Code_2
,Supplemental_Infrmtn2_Quantity AS Supplemental_Qty_2
,Quantity_Qualifier3_Code AS Supplemental_Qty_Qlfr_Code_3
,Supplemental_Infrmtn3_Quantity AS Supplemental_Qty_3
,Quantity_Qualifier4_Code AS Supplemental_Qty_Qlfr_Code_4
,Supplemental_Infrmtn4_Quantity AS Supplemental_Qty_4
,Quantity_Qualifier5_Code AS Supplemental_Qty_Qlfr_Code_5
,Supplemental_Infrmtn5_Quantity AS Supplemental_Qty_5
,Quantity_Qualifier6_Code AS Supplemental_Qty_Qlfr_Code_6
,Supplemental_Infrmtn6_Quantity AS Supplemental_Qty_6
,Quantity_Qualifier7_Code AS Supplemental_Qty_Qlfr_Code_7
,Supplemental_Infrmtn7_Quantity AS Supplemental_Qty_7
,Quantity_Qualifier8_Code AS Supplemental_Qty_Qlfr_Code_8
,Supplemental_Infrmtn8_Quantity AS Supplemental_Qty_8
,Quantity_Qualifier9_Code AS Supplemental_Qty_Qlfr_Code_9
,Supplemental_Infrmtn9_Quantity AS Supplemental_Qty_9
,Quantity_Qualifier10_Code AS Supplemental_Qty_Qlfr_Code_10
,Supplementl_Infrmtn10_Quantity AS Supplemental_Qty_10
,Quantity_Qualifier11_Code AS Supplemental_Qty_Qlfr_Code_11
,Supplementl_Infrmtn11_Quantity AS Supplemental_Qty_11
,Quantity_Qualifier12_Code AS Supplemental_Qty_Qlfr_Code_12
,Supplementl_Infrmtn12_Quantity AS Supplemental_Qty_12
,Quantity_Qualifier13_Code AS Supplemental_Qty_Qlfr_Code_13
,Supplementl_Infrmtn13_Quantity AS Supplemental_Qty_13
,Quantity_Qualifier14_Code AS Supplemental_Qty_Qlfr_Code_14
,Supplementl_Infrmtn14_Quantity AS Supplemental_Qty_14
,RARC_Code1 AS RARC_Code1
,RARC_Code2 AS RARC_Code2
,RARC_Code3 AS RARC_Code3
,RARC_Code4 AS RARC_Code4
,RARC_Code5 AS RARC_Code5
,'E' AS   Source_System_Code
,Current_Timestamp(0) AS DW_Last_Update_Date_Time
FROM edwpbs_staging.Remittance_Claim A
--left join Edwpf_staging.Clinical_AcctKeys CA
--ON CA.pat_acct_num = A.Remit_Account_Number
--AND  A.Provider_Split_COID=CA.coid
--and A.Provider_Split_Unit_num =CA.Unit_num
left  JOIN (Select * from edwpf_views.fact_patient where Source_System_code = 'P') fp
ON fp.pat_acct_num = A.Remit_Account_Number
AND  A.Provider_Split_COID=(case when fp.coid='08158' and fp.Sub_Unit_Num = 5 then '8165'   
 when fp.coid='34224' and fp.Sub_Unit_Num = 2 then '34241' else fp.coid end)
LEFT JOIN  edwpbs.Ref_Remittance_Subscriber_Info B ON 
Coalesce(B.Patient_Health_Ins_Num,'')=Coalesce(A.Ins_Subscriber_Id,'') AND 
Coalesce(B.Insured_Identification_Qualifier_Code,'')=Coalesce(A.Insurd_Identifictn_Qualifr_Cod,'') AND 
Coalesce(B.Subscriber_Id,'')=Coalesce(A.Subscriber_Id,'') AND         
Coalesce(B.Insured_Entity_Type_Qualifier_Code,'')=Coalesce(A.Insured_Entity_Typ_Qualifr_Cod,'') AND       
Coalesce(B.Subscriber_Last_Name,'')=Coalesce(A.Subscriber_Last_Name,'') AND 
Coalesce(B.Subscriber_First_Name,'')=Coalesce(A.Subscriber_First_Name,'') AND
Coalesce(B.Subscriber_Middle_Name,'')=Coalesce(A.Subscriber_Middle_Name,'') AND 
Coalesce(B.Subscriber_Name_Suffix,'')=Coalesce(A.Subscriber_Name_Suffix,'')
LEFT JOIN  EDWPBS.Ref_Remittance_Oth_Subscriber_Info C ON
Coalesce(A.Othr_Subcbr_Enty_Typ_Qulfr_Cod,'')=Coalesce(C.Oth_Subscriber_Enty_Type_Qualifier_Code,'') AND 
Coalesce(A.Other_Subscriber_Last_Name,'')=Coalesce(C.Oth_Subscriber_Last_Name,'') AND 
Coalesce(A.Other_Subscriber_First_Name,'')=Coalesce(C.Oth_Subscriber_First_Name,'') AND         
Coalesce(A.Other_Subscriber_Middle_Name,'')=Coalesce(C.Oth_Subscriber_Middle_Name,'') AND       
Coalesce(A.Other_Subscriber_Name_Suffix,'')=Coalesce(C.Oth_Subscriber_Name_Suffix,'') AND 
Coalesce(A.Othr_Subcbr_Idntfctn_Qulfr_Cod,'')=Coalesce(C.Oth_Subscriber_Id_Qualifier_Code,'') AND
Coalesce(A.Other_Subscriber_Id,'')=Coalesce(C.Oth_Subscriber_Id,'')  
LEFT JOIN  EDWPBS.Ref_Remittance_Rendering_Provider D ON
Coalesce(D.Serv_Provider_Enty_Type_Qualifier_Code,'')=Coalesce(A.Srvce_Prvdr_Enty_Typ_Qulfr_Cod,'') AND 
Coalesce(D.Rendering_Provider_Last_Org_Name,'')=Coalesce(A.Rendering_Provider_Last_Org_Nm,'') AND 
Coalesce(D.Rendering_Provider_First_Name,'')=Coalesce(A.Rendering_Provider_First_Name,'') AND         
Coalesce(D.Rendering_Provider_Middle_Name,'')=Coalesce(A.Rendering_Provider_Middle_Name,'') AND       
Coalesce(D.Rendering_Provider_Name_Suffix,'')=Coalesce(A.Rendering_Provider_Name_Suffix,'') AND 
Coalesce(D.Serv_Provider_Id_Qualifier_Code,'')=Coalesce(A.Srvce_Prvdr_Idntfctn_Qulfr_Cod,'') AND
Coalesce(D.Rendering_Provider_Id,'')=Coalesce(A.Rendering_Provider_Id,'')  
LEFT JOIN  EDWPBS.Ref_Remittance_COB_Carrier E ON
Coalesce(E.COB_Qualifier_Code,'')=Coalesce(A.Crossover_Carrier_Qualifr_Code,'') AND 
Coalesce(E.COB_Carrier_Num,'')=Coalesce(A.Cordintn_of_Benefit_Carrier_Nm,'') AND 
Coalesce(E.COB_Carrier_Name,'')=Coalesce(A.Coordintn_Of_Beneft_Carrier_Nm,'') 
LEFT JOIN  EDWPBS.Ref_Remittance_Corrected_Priority_Payor F ON
Coalesce(F.Corrected_Priority_Payor_Qualifier_Code,'')=Coalesce(A.Corretd_Prior_Payor_Qulifr_Cod,'') AND 
Coalesce(F.Corrected_Priority_Payor_Id,'')=Coalesce(A.Corrected_Priority_Payor_Num,'') AND 
Coalesce(F.Corrected_Priority_Payor_Name,'')=Coalesce(A.NM103_Corretd_Priority_Payr_Nm,'') 
WHERE A.DW_Last_Update_Date_Time =(SELECT Max(Cast(DW_Last_Update_Date_Time AS DATE)) FROM EDWPBS_staging.Remittance_Claim)) a;