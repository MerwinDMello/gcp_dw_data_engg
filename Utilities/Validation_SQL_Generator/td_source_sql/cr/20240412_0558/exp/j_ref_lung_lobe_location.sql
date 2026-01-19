
SELECT	'J_REF_LUNG_LOBE_LOCATION'||','|| cast(count(*) as varchar(20))||',	' as SOURCE_STRING 
FROM
(
SELECT 
trim(Lung_Lobe_Location_Desc) as Lung_Lobe_Location_Desc,
Source_System_Code as Source_System_Code
FROM EDWCR_Staging.Ref_Lung_Lobe_Location_stg   
where trim(Lung_Lobe_Location_Desc) not in (sel trim(Lung_Lobe_Location_Desc) from EDWCR.REF_LUNG_LOBE_LOCATION )
and DW_Last_Update_Date_Time < (SELECT MAX(Job_Start_Date_Time) as Job_Start_Date_Time FROM EDWCR_DMX_AC.ETL_JOB_RUN where Job_Name = 'J_REF_LUNG_LOBE_LOCATION')
) A;