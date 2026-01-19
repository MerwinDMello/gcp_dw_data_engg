SELECT 'J_EP_Ref_Remittance_Payor' || ',' || CAST(COUNT(*) AS VARCHAR(20)) || ',' as Source_String from
(
SELECT 
(SELECT Coalesce(Max(Remittance_Payor_SID),0)  FROM EDWPBS.REF_REMITTANCE_PAYOR) +
Row_Number() Over (ORDER BY 
Payment_Carrier_Num, Payment_Agency_Num ,           
Payor_Ref_Id , Payor_Name,Payor_City_Name,Payor_State_Code,Payor_Postal_Zone_Code
)AS Remittance_Payor_SID,
PAYMENT_CARRIER_NUM  AS Payment_Carrier_Num ,  
--EP_Payor_Num  ,
PAYMENT_AGENCY_NUM   AS Payment_Agency_Num,
PAYOR_REF_ID  AS Payor_Ref_Id, 
Payor_Name  AS Payor_Name ,     
Payor_Address_Line_1  AS Payor_Address_Line_1 ,   
Payor_Address_Line_2   AS Payor_Address_Line_2  ,       
Payor_City_Name  AS Payor_City_Name ,            
Payor_State_Code   AS Payor_State_Code ,   
Payor_Postal_Zone_Code    AS Payor_Postal_Zone_Code   ,
PAYOR_LINE_OF_BUSINESS  AS Payor_Line_Of_Business, 
PAYOR_ALTERNATE_REF_ID AS Payor_Alternate_Ref_Id,       
PAYOR_LONG_NAME  AS Payor_Long_Name ,           
PAYOR_SHORT_NAME  AS Payor_Short_Name ,        
/*Payor_Technical_Contact_Name , 
Payor_Primary_Comm_Type_Code  ,
Payor_Primary_Contact_Comm_Num ,
Payor_Secondary_Comm_Type_Code,
Payor_Secondary_Contact_Comm_Num,
Payor_Tertiary_Comm_Type_Code ,
Payor_Tertiary_Contact_Comm_Num,*/
'E' AS Source_System_Code ,           
Current_Timestamp(0) AS DW_Last_Update_Date_Time    
 FROM EDWPBS_STAGING.REMITTANCE_PAYMENT STG where DW_Last_Update_Date_Time = (Select Max(Cast(DW_Last_Update_Date_Time AS DATE))  from EDWPBS_staging.remittance_payment)
AND 
 ( Payment_Carrier_Num, Payment_Agency_Num ,Payor_Ref_Id , Payor_Name,
Payor_Address_Line_1,Payor_Address_Line_2,
Payor_City_Name,Payor_State_Code,Payor_Postal_Zone_Code,
Payor_Line_Of_Business,Payor_Alternate_Ref_Id,Payor_Long_Name,Payor_Short_Name) NOT IN (
SELECT Payment_Carrier_Num, Payment_Agency_Num_AN , Payor_Ref_Id , Payor_Name,
Payor_Address_Line_1,Payor_Address_Line_2,
Payor_City_Name,Payor_State_Code,Payor_Postal_Zone_Code,
Payor_Line_Of_Business,Payor_Alternate_Ref_Id,Payor_Long_Name,Payor_Short_Name
FROM EDWPBS.REF_REMITTANCE_PAYOR )
GROUP BY 
 Payment_Carrier_Num,           
--EP_Payor_Num ,                
Payment_Agency_Num ,           
Payor_Ref_Id ,                 
Payor_Name ,             
Payor_Address_Line_1 ,     
Payor_Address_Line_2 ,         
Payor_City_Name  ,             
Payor_State_Code ,             
Payor_Postal_Zone_Code  ,      
Payor_Line_Of_Business ,       
Payor_Alternate_Ref_Id,        
Payor_Long_Name    ,           
Payor_Short_Name    
)a
