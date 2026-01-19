SELECT CONCAT(count(*), ',	') AS SOURCE_STRING
FROM
 (SELECT Contact_Purpose_Desc,
 Source_System_Code
 FROM EDWCR_Staging.Contact_Purpose_stg
 WHERE trim(Contact_Purpose_Desc) NOT IN (sel trim(Contact_Purpose_Desc)
 FROM EdwCR.REF_CONTACT_PURPOSE)
 AND DW_Last_Update_Date_Time <
 (SELECT MAX(Job_Start_Date_Time) AS Job_Start_Date_Time
 FROM EDWCR_DMX_AC.ETL_JOB_RUN
 WHERE Job_Name = 'J_REF_CONTACT_PURPOSE') ) A;