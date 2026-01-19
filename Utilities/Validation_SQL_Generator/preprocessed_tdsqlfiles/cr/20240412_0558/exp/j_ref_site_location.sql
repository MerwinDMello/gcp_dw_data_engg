
SELECT CONCAT(count(*), ',	') AS SOURCE_STRING
FROM
 (SELECT Site_Location_Desc ,
 Source_System_Code
 FROM EDWCR_STAGING.Ref_Site_Location_STG
 WHERE trim(Site_Location_Desc) NOT IN (SEL TRIM(Site_Location_Desc)
 FROM EDWCR.Ref_Site_Location
 WHERE Site_Location_Desc IS NOT NULL)
 AND DW_Last_Update_Date_Time <
 (SELECT MAX(Job_Start_Date_Time) AS Job_Start_Date_Time
 FROM EDWCR_DMX_AC.ETL_JOB_RUN
 WHERE Job_Name = 'J_REF_SITE_LOCATION') ) A;