SELECT CONCAT(count(*)) AS SOURCE_STRING
FROM
 (SELECT ROW_NUMBER() OVER (
 ORDER BY CAST(DimSiteID AS INT),
 CAST(FactTreatmentHistoryID AS INT)) AS Fact_Treatment_History_SK,
 ra.Patient_Course_SK AS Patient_Course_SK,
 pp.Patient_Plan_SK AS Patient_Plan_SK,
 ra1.Patient_SK AS Patient_SK,
 DimLkpID_TreatmentIntentType ,
 DimLookupID_ClinicalStatus ,
 DimLookupID_PlanStatus,
 DimLookupID_FieldTechnique,
 DimLookupID_Technique,
 DimLookupID_TechniqueLabel,
 DimLkpID_TreatmentDeliveryTyp,
 rr.Site_SK AS Site_SK,
 FactTreatmentHistoryID,
 Completion_Date_Time,
 First_Treatment_Date_Time,
 Last_Treatment_Date_Time,
 Status_Date_Time,
 Active_Ind,
 PlannedDoseRate,
 CourseDoseDelivered,
 CourseDosePlanned,
 CourseDoseRemaining,
 OtherCourseDoseDelivered,
 DoseCorrection,
 TotalDoseLimit,
 DailyDoseLimit,
 SessionDoseLimit,
 Primary_Ind,
 LogID,
 RunID,
 'R' AS Source_System_Code,
 CURRENT_TIMESTAMP (0) AS DW_Last_Update_Date_Time
 FROM
 (SELECT DimSiteID,
 DimCourseID,
 DimPlanID,
 DimPatientID,
 DimLkpID_TreatmentIntentType ,
 DimLookupID_ClinicalStatus ,
 DimLookupID_PlanStatus,
 DimLookupID_FieldTechnique,
 DimLookupID_Technique,
 DimLookupID_TechniqueLabel,
 DimLkpID_TreatmentDeliveryTyp,
 FactTreatmentHistoryID,
 CAST(TRIM(CompletedDateTime) AS TIMESTAMP(0)) AS Completion_Date_Time,
 CAST (TRIM(FirstTreatmentDate) AS TIMESTAMP(0)) AS First_Treatment_Date_Time,
 CAST(TRIM(LastTreatmentDate) AS TIMESTAMP(0)) AS Last_Treatment_Date_Time,
 CAST(TRIM(StatusDate) AS timestamp(0)) AS Status_Date_Time,
 CASE
 WHEN IsActive =1 THEN 'Y'
 WHEN IsActive=0 THEN 'N'
 END AS Active_Ind,
 PlannedDoseRate,
 TRIM(CourseDoseDelivered) CourseDoseDelivered,
 TRIM(CourseDosePlanned) CourseDosePlanned,
 TRIM(CourseDoseRemaining) CourseDoseRemaining,
 TRIM(OtherCourseDoseDelivered) OtherCourseDoseDelivered,
 TRIM(DoseCorrection) DoseCorrection,
 TRIM(TotalDoseLimit) TotalDoseLimit,
 TRIM(DailyDoseLimit) DailyDoseLimit,
 TRIM(SessionDoseLimit) SessionDoseLimit,
 CASE
 WHEN PrimaryFlag =1 THEN 'Y'
 WHEN PrimaryFlag=0 THEN 'N'
 END AS Primary_Ind,
 LogID,
 RunID
 FROM EDWCR_Staging.FactTreatmentHist_STG) dp
 INNER JOIN
 (SELECT Source_Site_Id,
 Site_SK
 FROM EDWCR_BASE_VIEWS.Ref_Rad_Onc_Site) rr ON rr.Source_Site_Id=dp.DimSiteID
 LEFT OUTER JOIN
 (SELECT (Cast(Patient_Course_SK AS INT)) AS Patient_Course_SK,
 Source_Patient_Course_Id,
 Site_SK
 FROM EDWCR_BASE_VIEWS.Rad_Onc_Patient_Course) ra ON dp.DimCourseID = ra.Source_Patient_Course_Id
 AND rr.Site_Sk=ra.Site_SK
 LEFT OUTER JOIN
 (SELECT Source_Patient_Plan_Id,
 Site_SK,
 Patient_Plan_SK
 FROM EDWCR_BASE_VIEWS.Rad_Onc_patient_plan) pp ON dp.DimPlanID = pp.Source_Patient_Plan_Id
 AND rr.Site_Sk=pp.Site_SK
 LEFT OUTER JOIN
 (SELECT Source_Patient_Id,
 Site_Sk,
 Patient_SK
 FROM EDWCR_BASE_VIEWS.Rad_Onc_patient) ra1 ON dp.DimPatientID = ra1.Source_Patient_Id
 AND rr.Site_Sk=ra1.Site_SK)stg