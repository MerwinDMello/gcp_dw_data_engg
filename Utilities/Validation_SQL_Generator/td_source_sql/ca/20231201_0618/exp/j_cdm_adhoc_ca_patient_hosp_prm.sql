
select '$JOBNAME' || ',' || cast(count(*) as varchar(30)) || ',' as SOURCE_STRING FROM (
	SELECT DISTINCT
		 CAP.Patient_SK 
		, CAH.Hospital_SK
		, CAS.Server_SK
		,  CAHS.HospitalizationID AS	Source_Patient_Hosp_Id
		,  CAHS.AttendSurg	AS Attending_Consultant_Id
		,  CAHS.HICNumber AS	HIC_Num
		, CAST(SUBSTR(CAST(CAHS.AdmitDt AS VARCHAR(23)),1,10 ) || SUBSTR(CAST(CAHS.AdmitTime AS VARCHAR(23)),11,9) AS TIMESTAMP(0)) AS	Admission_Date_Time
		,  CAHS.DisLoctn AS	Discharge_Location_Id
		,  CAST(SUBSTR(CAST(CAHS.DBDischDt AS VARCHAR(23)),1,10) AS Date) AS	Db_Discharge_Date
		,  CAHS.MtDBDisStat AS	Db_Discharge_Mortality_Status_Id
		,  CAHS.Readmit30 AS	Readmission_30_Day_Id
		,  CAST(SUBSTR(CAST(CAHS.DischDt AS VARCHAR(23)),1,10) AS Date) AS	Discharge_Date
		,  CAST(SUBSTR(CAST(CAHS.ReadmitDt AS VARCHAR(23)),1,10) AS Date) AS	Readmission_Date
		,  CAHS.MtDCStat AS	Discharge_Mortality_Status_Id
		,  CAHS.ReadmitRsn AS	Readmission_Reason_Id
		,  CAST(SUBSTR(CAST(CAHS.CreateDate AS VARCHAR(23)),1,19) AS TIMESTAMP(0)) AS	Source_Create_Date_Time
		,  CAST(SUBSTR(CAST(CAHS.LastUpdate AS VARCHAR(23)),1,19) AS TIMESTAMP(0)) AS	Source_Last_Update_Date_Time
		,  CAHS.UpdateBy AS	Updated_By_3_4_Id
		,  CAHS.AcctNum AS	Pat_Acct_Num_AN
		,  CAHS.InsPrimType AS	Primary_Insurance_Type_Id
		,  CAHS.AdmitFromLoc AS	Admission_Location_Id
	FROM EDWCDM_STAGING.CardioAccess_Hospitalization_STG CAHS
		INNER JOIN EDWCDM.CA_SERVER CAS
			ON CAHS.Full_Server_Nm = CAS.Server_Name
		LEFT OUTER JOIN EDWCDM.CA_Patient CAP
			ON CAHS.PatId = CAP.Source_Patient_Id
				AND CAS.Server_SK = CAP.Server_SK
		LEFT OUTER JOIN EDWCDM.CA_Hospital CAH
			ON CAHS.HospName = CAH.Source_Hospital_ID
				AND CAS.Server_SK = CAH.Server_SK
) a