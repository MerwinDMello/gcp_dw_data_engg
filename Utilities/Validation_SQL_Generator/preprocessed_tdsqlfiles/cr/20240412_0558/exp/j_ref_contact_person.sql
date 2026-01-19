SELECT CONCAT(count(*), ',	') AS SOURCE_STRING
FROM
 (SELECT Contact_Person_Desc,
 Source_System_Code
 FROM EDWCR_Staging.Contact_Person_stg
 WHERE trim(Contact_Person_Desc) NOT IN (sel trim(Contact_Person_Desc)
 FROM EdwCR.REF_CONTACT_PERSON)
 AND DW_Last_Update_Date_Time <
 (SELECT MAX(Job_Start_Date_Time) AS Job_Start_Date_Time
 FROM EDWCR_DMX_AC.ETL_JOB_RUN
 WHERE Job_Name = 'J_REF_CONTACT_PERSON') ) A;