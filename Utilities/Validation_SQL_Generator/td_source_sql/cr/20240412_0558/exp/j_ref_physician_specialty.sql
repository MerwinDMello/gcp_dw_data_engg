
SELECT	'J_REF_PHYSICIAN_SPECIALTY'||','|| cast(count(*) as varchar(20))||',	' as SOURCE_STRING 
FROM
(
SELECT 
trim(Physician_Speciality_Desc) as Physician_Speciality_Desc,
Source_System_Code as Source_System_Code
FROM EDWCR_Staging.REF_PHYSICIAN_SPECIALTY_Stg   
where trim(Physician_Speciality_Desc) not in (sel trim(Physician_Specialty_Desc) from EDWCR.REF_PHYSICIAN_SPECIALTY )
and DW_Last_Update_Date_Time < (SELECT MAX(Job_Start_Date_Time) as Job_Start_Date_Time FROM EDWCR_DMX_AC.ETL_JOB_RUN where Job_Name = 'J_REF_PHYSICIAN_SPECIALTY')
) A;