
SELECT	'J_CN_PATIENT_CONTACT'||','|| cast(count(*) as varchar(20))||',	' as SOURCE_STRING 
FROM
(
SELECT 
CN_Patient_Contact_SID                
,Hashbite_SSK                  
FROM edwcr_staging.CN_Patient_Contact_stg
where Hashbite_SSK not in ( Select Hashbite_SSK from edwcr.CN_PATIENT_CONTACT )
and DW_Last_Update_Date_Time < (SELECT MAX(Job_Start_Date_Time) as Job_Start_Date_Time FROM EDWCR_DMX_AC.ETL_JOB_RUN where Job_Name = 'J_CN_PATIENT_CONTACT')
) A;