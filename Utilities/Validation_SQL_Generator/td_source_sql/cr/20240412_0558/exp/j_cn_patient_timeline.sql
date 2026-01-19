SELECT 'J_CN_PATIENT_TIMELINE'||','|| cast(count(*) as varchar(20))||',	' as SOURCE_STRING 
FROM
(
Select
        CN_Patient_Timeline_Id,
      Nav_Patient_Id ,
      Tumor_Type_Id ,
      Navigator_Id ,
      Coid ,
      Company_Code ,
      Nav_Referred_Date ,
      First_Treatment_Date,
      First_Consult_Date ,
      First_Imaging_Date ,
      First_Medical_Oncology_Date ,
      First_Radiation_Oncology_Date ,
      First_Diagnosis_Date,
      First_Biopsy_Date ,
      First_Surgery_Consult_Date ,
      First_Surgery_Date,
      Surv_Care_Plan_Close_Date ,
      Surv_Care_Plan_Resolve_Date ,
      End_Treatment_Date,
      Death_Date,
      Diag_First_Trt_Day_Num ,
      Diag_First_Trt_Available_Ind ,
      Hashbite_SSK ,
      Source_System_Code ,
      CURRENT_TIMESTAMP(0) AS DW_Last_Update_Date_Time
	  
From edwcr_staging.CN_PATIENT_TIMELINE_STG  
where Hashbite_SSK not in ( Select Hashbite_SSK from edwcr.CN_PATIENT_TIMELINE)
)A;