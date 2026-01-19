SELECT 'J_CN_PATIENT_MLTP_DISP_MEETING'||','|| cast(count(*) as varchar(20))||',	' as SOURCE_STRING 
FROM
(
Select
      CN_Patient_Mltp_Disc_Meet_SID,
      Nav_Patient_Id,
      Tumor_Type_Id,
      Navigator_Id,
      Coid,
      Company_Code,
      Meeting_Date,
      Patient_Discussed_Ind,
      Treatment_Change_Ind,
      Meeting_Notes_Text,
      Hashbite_SSK,
      Source_System_Code,
      DW_Last_Update_Date_Time
From edwcr_staging.CN_Patient_Mltp_Disciplinary_Meeting_STG STG 
where STG.Hashbite_SSK not in ( Select Hashbite_SSK from edwcr.CN_Patient_Mltp_Disciplinary_Meeting)
)A;