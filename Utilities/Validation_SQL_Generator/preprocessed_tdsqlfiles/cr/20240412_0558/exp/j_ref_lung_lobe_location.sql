
SELECT CONCAT(count(*), ',	') AS SOURCE_STRING
FROM
 (SELECT trim(Lung_Lobe_Location_Desc) AS Lung_Lobe_Location_Desc,
 Source_System_Code AS Source_System_Code
 FROM EDWCR_Staging.Ref_Lung_Lobe_Location_stg
 WHERE trim(Lung_Lobe_Location_Desc) NOT IN (sel trim(Lung_Lobe_Location_Desc)
 FROM EDWCR.REF_LUNG_LOBE_LOCATION)
 AND DW_Last_Update_Date_Time <
 (SELECT MAX(Job_Start_Date_Time) AS Job_Start_Date_Time
 FROM EDWCR_DMX_AC.ETL_JOB_RUN
 WHERE Job_Name = 'J_REF_LUNG_LOBE_LOCATION') ) A;