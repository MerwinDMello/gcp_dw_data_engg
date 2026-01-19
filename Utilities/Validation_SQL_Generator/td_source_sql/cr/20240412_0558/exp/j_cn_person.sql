SELECT 'J_CN_PERSON'||','|| cast(count(*) as varchar(20))||', ' as SOURCE_STRING 
FROM
(
SELECT 
Nav_Patient_Id
,Birth_Date
,First_Name
,Last_Name
,Middle_Name
,Perferred_Name
,Gender_Code
,Preferred_Langauage_Text
,Death_Date
,Patient_Email_Text
,'N'Source_System_Code
,DW_Last_Update_Date_Time
FROM edwcr_staging.CN_PERSON_Stg
where Nav_Patient_Id not in ( Select Nav_Patient_Id from edwcr_base_views.CN_PERSON)
)SRC;