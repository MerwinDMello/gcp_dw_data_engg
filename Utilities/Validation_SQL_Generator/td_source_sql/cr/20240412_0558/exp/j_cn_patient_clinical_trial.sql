SELECT 'J_CN_PATIENT_CLINICAL_TRIAL'||','|| cast(count(*) as varchar(20))||',	' as SOURCE_STRING 
FROM
(
SELECT 
CN_Patient_Clinical_Trial_SID 
,Nav_Patient_Id                
,Tumor_Type_Id                 
,Diagnosis_Result_Id           
,Nav_Diagnosis_Id              
,Navigator_Id                  
,Coid                          
,Company_Code                  
,Clinical_Trial_Name           
,Clinical_Trial_Enrolled_Ind   
,Clinical_Trial_Enrolled_Date  
,Clinical_Trial_Offered_Ind    
,Clinical_Trial_Offered_Date   
,Hashbite_SSK                  
,Source_System_Code            
,Current_timestamp(0) as DW_Last_Update_Date_Time      
FROM edwcr_staging.CN_Patient_Clinical_Trial_Stg
where Hashbite_SSK not in ( Select Hashbite_SSK from edwcr.CN_Patient_Clinical_Trial )
) A;