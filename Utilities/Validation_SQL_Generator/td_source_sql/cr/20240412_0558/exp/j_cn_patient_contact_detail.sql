SELECT 'J_CN_PATIENT_CONTACT_DETAIL'||','|| cast(count(*) as varchar(20))||',	' as SOURCE_STRING 
FROM
(
Select
      CN_Patient_Contact_SID,
      Contact_Detail_Measure_Type_Id,
      Contact_Detail_Measure_Val_Txt,
      Hashbite_SSK,
      Source_System_Code,
      DW_Last_Update_Date_Time
From edwcr_staging.CN_PATIENT_CONTACT_DETAIL_STG STG 
where STG.Hashbite_SSK not in ( Select Hashbite_SSK from edwcr.CN_PATIENT_CONTACT_DETAIL)
)A;