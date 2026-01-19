export JOBNAME='J_CR_RO_FACT_RAD_ONC_PATIENT'

export AC_EXP_SQL_STATEMENT="SELECT 'J_CR_RO_FACT_RAD_ONC_PATIENT'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING 
FROM EDWCR_STAGING.STG_FactPatient"

export AC_ACT_SQL_STATEMENT="select 'J_CR_RO_FACT_RAD_ONC_PATIENT'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING 
FROM 
(
select 
Row_Number() Over (Order By Cast(DHD.DimSiteID as Int) ,Cast(DHD.FactPatientID as Int)) as Fact_Patient_SK,
rpp.Hospital_SK as Hospital_SK,
rpa.Patient_Sk as Patient_SK,
DHD.Patient_Status_Id as Patient_Status_Id ,
DHD.Location_SK,
DHD.Race_Id,
DHD.Gender_Id,
RR.Site_SK as Site_SK,
DHD.FactPatientID as Source_Fact_Patient_Id,
DHD.Creation_Date_Time,
DHD.Admission_Date_Time,
DHD.Discharge_Date_Time,
DHD.Log_Id,
DHD.Run_Id,
'R' as Source_System_Code,
CURRENT_TIMESTAMP(0) as DW_Last_Update_Date_Time
FROM
(
Select
distinct Cast(DimSiteID as Int) as  DimSiteID ,Cast(FactPatientID as Int) as FactPatientID,
DimHospitalDepartmentID,
DimPatientID,
CAST(DimLookupID_PatientStatus as INT)as Patient_Status_Id,
CAST(DimLocationID AS INT) as Location_SK,
cast(DimLookupID_Race as INT) as Race_Id,
CAST(DimLookupID_Gender as INT) as Gender_Id,
CAST(cast(PatientCreationDate as varchar(19)) as timestamp(0)) as Creation_Date_Time,
CAST(cast(PatientAdmissionDate as varchar(19)) as timestamp(0)) as Admission_Date_Time,
CAST(cast(PatientDischargeDate as varchar(19)) as timestamp(0)) as Discharge_Date_Time,
Cast(LogID as Int) as Log_Id,
Cast(RunID as Int) as Run_Id
FROM EDWCR_STAGING.STG_FactPatient 
) DHD
LEFT OUTER JOIN EDWCR_BASE_VIEWS.Ref_Rad_Onc_Hospital rpp
ON CAST(DHD.DimHospitalDepartmentID as INT)=rpp.Source_Hospital_Id
and Cast(DHD.DimSiteID as Int) =rpp.Site_SK
left outer join EDWCR_BASE_VIEWS.Rad_Onc_Patient rpa
ON Cast(DHD.DimPatientID as Int) = rpa.Source_Patient_Id
and Cast(DHD.DimSiteID as Int) =rpa.Site_SK
INNER JOIN EDWCR_BASE_VIEWS.REF_RAD_ONC_SITE RR
On RR.Source_Site_Id = Cast(DHD.DimSiteID as Int)
)DS
where 
DW_Last_Update_Date_Time >= (SELECT MAX(Job_Start_Date_Time) as Job_Start_Date_Time FROM ${NCR_AC_SCHEMA}.ETL_JOB_RUN where Job_Name = '$JOBNAME')
"