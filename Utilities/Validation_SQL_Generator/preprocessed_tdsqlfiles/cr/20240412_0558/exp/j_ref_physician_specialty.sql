
SELECT CONCAT(count(*), ',	') AS SOURCE_STRING
FROM
 (SELECT trim(Physician_Speciality_Desc) AS Physician_Speciality_Desc,
 Source_System_Code AS Source_System_Code
 FROM EDWCR_Staging.REF_PHYSICIAN_SPECIALTY_Stg
 WHERE trim(Physician_Speciality_Desc) NOT IN (sel trim(Physician_Specialty_Desc)
 FROM EDWCR.REF_PHYSICIAN_SPECIALTY)
 AND DW_Last_Update_Date_Time <
 (SELECT MAX(Job_Start_Date_Time) AS Job_Start_Date_Time
 FROM EDWCR_DMX_AC.ETL_JOB_RUN
 WHERE Job_Name = 'J_REF_PHYSICIAN_SPECIALTY') ) A;