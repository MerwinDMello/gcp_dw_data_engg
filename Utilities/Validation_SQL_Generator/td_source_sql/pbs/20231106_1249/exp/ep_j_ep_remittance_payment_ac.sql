SELECT 'J_EP_Remittance_Payment' || ',' || CAST(COUNT(*) AS VARCHAR(20)) || ',' as Source_String from
(
SELECT 	
Alternate_SN_Format_Ind AS Alternate_SN_Format_Ind,
APC_Amt AS APC_Amt,
Apply_ER_Patient_Rule_Ind AS Apply_ER_Patient_Rule_Ind,
Apply_NonCvrd_Charges_Rule_Ind AS Apply_NonCovered_Charges_Rule_Ind,
Apply_Recover_Rule_Ind AS Apply_Recover_Rule_Ind,
Apply_Secondary_Rule_Ind AS Apply_Secondary_Rule_Ind,
Apply_Total_Charges_Rule_Ind AS Apply_Total_Charges_Rule_Ind,
Audit_Date AS Audit_Date,
Bill_Charge_Amt AS Bill_Charge_Amt,
Bill_Process_Date AS Bill_Process_Date,
Bill_Type_Code AS Bill_Type_Code,
Calculated_Covered_Days_Ind AS Calculated_Covered_Days_Ind,
Capital_Amt AS Capital_Amt,
Check_Amt AS Check_Amt,
Check_Date AS Check_Date,
Check_Num AS Check_Num,
Claim_Cnt AS Claim_Cnt,
Claim_Payment_Amt AS Claim_Payment_Amt,
Coid AS Coid,
Comment_Transaction_Ind AS Comment_Transaction_Ind,
Company_Code AS Company_Code,
Cost_Report_Day_Cnt AS Cost_Report_Day_Cnt,
Covered_Charge_Amt AS Covered_Charge_Amt,
Delete_Date AS Delete_Date,
Delete_Ind AS Delete_Ind,
Discharge_Date_Replacement_Ind AS Discharge_Date_Replacement_Ind,
Disproportionate_Share_Amt AS Disproportionate_Share_Amt,
DRG_Amt AS DRG_Amt,
DRG_Replacement_Ind AS DRG_Replacement_Ind,
Current_Timestamp(0) AS DW_Last_Update_Date_Time,
EP_Calc_Blood_Deductible_Amt AS EP_Calc_Blood_Deductible_Amt,
EP_Calc_Covered_Day_Cnt AS EP_Calc_Covered_Day_Cnt,
EP_Calc_HCPCS_Charge_Amt AS EP_Calc_HCPCS_Charge_Amt,
EP_Calc_HCPCS_Payment_Amt AS EP_Calc_HCPCS_Payment_Amt,
EP_Coinsurance_Amt AS EP_Coinsurance_Amt,
EP_Contractual_Adj_Amt AS EP_Contractual_Adj_Amt,
EP_Create_Date AS EP_Create_Date,
EP_Deductible_Amt AS EP_Deductible_Amt,
EP_Denial_Amt AS EP_Denial_Amt,
EP_Discharge_Cnt AS EP_Discharge_Cnt,
EP_Effective_Post_Date AS EP_Effective_Post_Date,
EP_Lab_Charge_Amt AS EP_Lab_Charge_Amt,
EP_Lab_Payment_Amt AS EP_Lab_Payment_Amt,
EP_Non_Covered_Charge_Amt AS EP_Non_Covered_Charge_Amt,
EP_Nonpayable_Professional_Fee AS EP_Nonpayable_Professional_Fee_Amt,
EP_Payor_Batch_Code AS EP_Payor_Batch_Code,
EP_PLB_Key_Num AS EP_PLB_Key_Num,
EP_Primary_Insurance_Payment AS EP_Primary_Insurance_Payment_Amt,
EP_Therapy_Charge_Amt AS EP_Therapy_Charge_Amt,
EP_Therapy_Payment_Amt AS EP_Therapy_Payment_Amt,
ERA_Create_Date AS ERA_Create_Date,
Federal_Specific_DRG_Amt AS Federal_Specific_DRG_Amt,
Indirect_Teaching_Amt AS Indirect_Teaching_Amt,
Ins_Covered_Day_Cnt AS Ins_Covered_Day_Cnt,
Interchange_Receiver_Code AS Interchange_Receiver_Code,
Interchange_Receiver_ID AS Interchange_Receiver_ID,
Interchange_Recvr_Id_Qual_Code AS Interchange_Receiver_Id_Qualifier_Code,
Interchange_Sender_Code AS Interchange_Sender_Code,
Interchange_Sender_Id AS Interchange_Sender_Id,
Interchange_Sender_Qual_Code AS Interchange_Sender_Qualifier_Code,
Interest_Amt AS Interest_Amt,
Internal_Denial_Transctn_Ind AS Internal_Denial_Transaction_Ind,
Lab_Transaction_Breakout_Ind AS Lab_Transaction_Breakout_Ind,
NonCovered_Day_Cnt AS NonCovered_Day_Cnt,
Outlier_Amt AS Outlier_Amt,
Patient_Liability_Amt AS Patient_Liability_Amt,
Patient_Type_Replacement_Ind AS Patient_Type_Replacement_Ind,
Payment_GUID AS Payment_GUID,
Payor_Default_LoggingType_Code AS Payor_Default_Logging_Type_Code,
Payor_Live_Ind AS Payor_Live_Ind,
PLB_Recovery_Transaction_Ind AS PLB_Recovery_Transaction_Ind,
pply_Mother_Baby_Rule_Ind AS Apply_Mother_Baby_Rule_Ind,
Provider_level_Adj_Id AS Provider_level_Adj_Id,
RAC_Ind AS RAC_Ind,
B.Remittance_Payee_SID AS Remittance_Payee_SID,
C.Remittance_Payor_SID AS Remittance_Payor_SID,
Remittance_Seq_Num AS Remittance_Seq_Num,
'E' AS Source_System_Code,
Total_Charges_Replacement_Ind AS Total_Charges_Replacement_Ind,
Transmission_Receiving_Code AS Transmission_Receiving_Code,
Transmission_Sending_Code AS Transmission_Sending_Code,
Unit_Num AS Unit_Num
 FROM EDWPBS_STAGING.REMITTANCE_PAYMENT A
   
  LEFT JOIN  EDWPBS.REF_REMITTANCE_PAYEE B ON 
  
Coalesce(A.Payee_Name,'')=Coalesce(B.Payee_Name,'') AND 
Coalesce(A.Payee_Identification_Qual_Code,'')=Coalesce(B.Payee_Identification_Qualifier_Code,'') AND 
Coalesce(A.Payee_City_Name,'')=Coalesce(B.Payee_City_Name,'') AND         
Coalesce(A.Payee_State_Code,'')=Coalesce(B.Payee_State_Code,'') AND       
Coalesce(A.Payee_Postal_Zone_Code,'')=Coalesce(B.Payee_Postal_Zone_Code,'') AND 
Coalesce(A.Provider_Tax_Id_Lookup_Code,'')=Coalesce(B.Provider_Tax_Id_Lookup_Code,'') AND
CASE WHEN Length(Trim(A.PROVIDER_TAX_ID )) = 10 THEN  Trim(A.PROVIDER_TAX_ID) ELSE Coalesce(NULL,'') end  =Coalesce(B.Provider_NPI,'') AND
CASE WHEN Length(Trim(A.PROVIDER_TAX_ID )) = 9 THEN Trim(A.PROVIDER_TAX_ID )
WHEN Length(Trim(A.PROVIDER_TAX_ID)) = 8 THEN '0'||Trim(A.PROVIDER_TAX_ID)
 ELSE Coalesce(NULL,'') end =Coalesce(B.Provider_Tax_Id,'')
  LEFT JOIN  EDWPBS.REF_REMITTANCE_PAYOR C ON
  
  		Coalesce(A.	Payment_Carrier_Num,'')           	=		Coalesce(C.	Payment_Carrier_Num,'')           			AND
		Coalesce(A.	Payment_Agency_Num,'')            	=		Coalesce(C.	Payment_Agency_Num_AN,'')            			AND
		Coalesce(A.	Payor_Ref_Id,'')                  	=		Coalesce(C.	Payor_Ref_Id ,'')                 			AND
		Coalesce(A.	Payor_Name,'')             	=		Coalesce(C.	Payor_Name      ,'')        			AND
		Coalesce(A.	Payor_Address_Line_1,'')      	=		Coalesce(C.	Payor_Address_Line_1   ,'')   			AND
		Coalesce(A.	Payor_Address_Line_2,'')          	=		Coalesce(C.	Payor_Address_Line_2     ,'')     			AND
		Coalesce(A.	Payor_City_Name ,'')              	=		Coalesce(C.	Payor_City_Name         ,'')      			AND
		Coalesce(A.	Payor_State_Code,'')              	=		Coalesce(C.	Payor_State_Code   ,'')           			AND
		Coalesce(A.	Payor_Postal_Zone_Code,'')        	=		Coalesce(C.	Payor_Postal_Zone_Code,'')        			AND
		Coalesce(A.	Payor_Line_Of_Business,'')        	=		Coalesce(C.	Payor_Line_Of_Business    ,'')    			AND
		Coalesce(A.	Payor_Alternate_Ref_Id,'')        	=		Coalesce(C.	Payor_Alternate_Ref_Id   ,'')     			AND
		Coalesce(A.	Payor_Long_Name,'')               	=		Coalesce(C.	Payor_Long_Name       ,'')        			AND
		Coalesce(A.	Payor_Short_Name  ,'')    	=		Coalesce(C.	Payor_Short_Name   ,'')   			
WHERE A.DW_Last_Update_Date_Time =(SELECT Max(Cast(DW_Last_Update_Date_Time AS DATE)) FROM EDWPBS_staging.remittance_payment)
  )A
