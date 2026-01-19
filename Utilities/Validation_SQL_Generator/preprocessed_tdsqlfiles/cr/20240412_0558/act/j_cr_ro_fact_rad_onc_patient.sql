SELECT CONCAT(count(*)) AS SOURCE_STRING
FROM
 (SELECT Row_Number() OVER (
 ORDER BY Cast(DHD.DimSiteID AS Int),
 Cast(DHD.FactPatientID AS Int)) AS Fact_Patient_SK,
 rpp.Hospital_SK AS Hospital_SK,
 rpa.Patient_Sk AS Patient_SK,
 DHD.Patient_Status_Id AS Patient_Status_Id,
 DHD.Location_SK,
 DHD.Race_Id,
 DHD.Gender_Id,
 RR.Site_SK AS Site_SK,
 DHD.FactPatientID AS Source_Fact_Patient_Id,
 DHD.Creation_Date_Time,
 DHD.Admission_Date_Time,
 DHD.Discharge_Date_Time,
 DHD.Log_Id,
 DHD.Run_Id,
 'R' AS Source_System_Code,
 CURRENT_TIMESTAMP(0) AS DW_Last_Update_Date_Time
 FROM
 (SELECT DISTINCT Cast(DimSiteID AS Int) AS DimSiteID,
 Cast(FactPatientID AS Int) AS FactPatientID,
 DimHospitalDepartmentID,
 DimPatientID,
 CAST(DimLookupID_PatientStatus AS INT)AS Patient_Status_Id,
 CAST(DimLocationID AS INT) AS Location_SK,
 cast(DimLookupID_Race AS INT) AS Race_Id,
 CAST(DimLookupID_Gender AS INT) AS Gender_Id,
 CAST(PatientCreationDate AS timestamp(0)) AS Creation_Date_Time,
 CAST(PatientAdmissionDate AS timestamp(0)) AS Admission_Date_Time,
 CAST(PatientDischargeDate AS timestamp(0)) AS Discharge_Date_Time,
 Cast(LogID AS Int) AS Log_Id,
 Cast(RunID AS Int) AS Run_Id
 FROM EDWCR_STAGING.STG_FactPatient) DHD
 LEFT OUTER JOIN EDWCR_BASE_VIEWS.Ref_Rad_Onc_Hospital rpp ON CAST(DHD.DimHospitalDepartmentID AS INT)=rpp.Source_Hospital_Id
 AND Cast(DHD.DimSiteID AS Int) =rpp.Site_SK
 LEFT OUTER JOIN EDWCR_BASE_VIEWS.Rad_Onc_Patient rpa ON Cast(DHD.DimPatientID AS Int) = rpa.Source_Patient_Id
 AND Cast(DHD.DimSiteID AS Int) =rpa.Site_SK
 INNER JOIN EDWCR_BASE_VIEWS.REF_RAD_ONC_SITE RR ON RR.Source_Site_Id = Cast(DHD.DimSiteID AS Int))DS
WHERE DW_Last_Update_Date_Time >=
 (SELECT MAX(Job_Start_Date_Time) AS Job_Start_Date_Time
 FROM edwcr_dmx_ac.ETL_JOB_RUN
 WHERE Job_Name = 'J_CR_RO_FACT_RAD_ONC_PATIENT')