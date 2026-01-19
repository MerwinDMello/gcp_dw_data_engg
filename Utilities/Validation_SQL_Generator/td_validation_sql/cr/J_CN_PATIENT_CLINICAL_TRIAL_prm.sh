export JOBNAME='J_CN_PATIENT_CLINICAL_TRIAL'

export AC_EXP_SQL_STATEMENT="SELECT 'J_CN_PATIENT_CLINICAL_TRIAL'||','|| cast(count(*) as varchar(20))||',	' as SOURCE_STRING 
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
FROM $NCR_STG_SCHEMA.CN_Patient_Clinical_Trial_Stg
where Hashbite_SSK not in ( Select Hashbite_SSK from $NCR_TGT_SCHEMA.CN_Patient_Clinical_Trial )
) A;"

export AC_ACT_SQL_STATEMENT="select 'J_CN_PATIENT_CLINICAL_TRIAL'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING 
FROM $EDWCR_BASE_VIEWS.CN_Patient_Clinical_Trial  WHERE 
DW_Last_Update_Date_Time >= (SELECT MAX(Job_Start_Date_Time) as Job_Start_Date_Time FROM EDWCR_DMX_AC_BASE_VIEWS.ETL_JOB_RUN where Job_Name = 'J_CN_PATIENT_CLINICAL_TRIAL');"
