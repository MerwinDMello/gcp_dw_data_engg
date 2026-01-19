export JOBNAME='J_CDM_ADHOC_CA_Junc_Patient_Address'

export AC_EXP_SQL_STATEMENT="
select '$JOBNAME' || ',' || cast(count(*) as varchar(30)) || ',' as SOURCE_STRING FROM (

SELECT DISTINCT CAP.PATIENT_SK
,'R' as Address_Type_Code
,CAA.Address_SK
FROM EDWCDM_STAGING.CardioAccess_Demographics_STG CADS
	INNER JOIN EDWCDM.CA_SERVER CAS
		ON CADS.Full_Server_Nm = CAS.Server_Name
	INNER JOIN EDWCDM.CA_Patient CAP
		ON CADS.PatId = CAP.Source_Patient_Id
			AND CAS.Server_SK = CAP.Server_SK
	INNER JOIN EDWCDM.CA_Address CAA
		ON COALESCE(PatAddr,'NULL') = COALESCE(Address_Line_1_Text,'NULL')
			AND COALESCE(CADS.PatAddr2,'NULL') = COALESCE(CAA.Address_Line_2_Text,'NULL')
			AND COALESCE(CADS.PatCity,'NULL') = COALESCE(CAA.City_Name,'NULL')
			AND COALESCE(CADS.PatState,'NULL') = COALESCE(CAA.State_Name,'NULL')
			AND COALESCE(CADS.PatZip,'NULL') = COALESCE(CAA.Zip_Code,'NULL')
			AND COALESCE(CADS.County,'NULL') = COALESCE(CAA.County_Name,'NULL')
			AND COALESCE(CADS.Country,'NULL') = COALESCE(CAA.Country_Id ,'NULL')
			AND CAA.Address_Line_3_Text is null
UNION
SELECT DISTINCT CAP.PATIENT_SK
,'B' as Address_Type_Code
,CAA.Address_SK
FROM EDWCDM_STAGING.CardioAccess_Demographics_STG CADS
	INNER JOIN EDWCDM.CA_SERVER CAS
		ON CADS.Full_Server_Nm = CAS.Server_Name
	INNER JOIN EDWCDM.CA_Patient CAP
		ON CADS.PatId = CAP.Source_Patient_Id
			AND CAS.Server_SK = CAP.Server_SK
	INNER JOIN EDWCDM.CA_Address CAA
		ON COALESCE(BirthCit,'NULL') = COALESCE(City_Name,'NULL')
			AND COALESCE(CADS.BirthSta,'NULL') = COALESCE(CAA.State_Name,'NULL')
			AND COALESCE(CADS.BirthZip,'NULL') = COALESCE(CAA.Zip_Code,'NULL')
			AND COALESCE(CADS.BirthCou,'NULL') = COALESCE(CAA.Country_Id ,'NULL')
			AND Address_Line_1_Text IS NULL 
			AND Address_Line_2_Text IS NULL 
			AND Address_Line_3_Text IS NULL 
			AND County_Name IS NULL
 ) b"

export AC_ACT_SQL_STATEMENT="
select '$JOBNAME'  || ','  || cast(COUNT(*) as varchar(30)) ||',' as SOURCE_STRING FROM (
Select * from  EDWCDM.CA_Junc_Patient_Address
where DW_Last_Update_Date_Time >= (SELECT Max(Job_Start_date_Time) FROM EDWCDM_DMX_AC.etl_job_run WHERE JOB_NAME = '$JOBNAME' AND Job_Start_date_Time IS NOT NULL )
) a"

