export JOBNAME='J_CDM_ADHOC_CA_Hospital'

export AC_EXP_SQL_STATEMENT="
select '$JOBNAME' || ',' || cast(count(*) as varchar(30)) || ',' as SOURCE_STRING FROM (
SELECT DISTINCT * FROM (
SELECT Distinct
	  null AS Hospital_SK,
	  ca.Address_SK AS Hospital_Address_SK,
	  csrv.Server_SK  AS Server_SK,
	  chs.HospitalID AS Source_Hospital_Id,
	  chs.OrganizationID AS Organization_Id,
	  chs.HospName AS Hospital_Name,
	  chs.HospNPI AS Hospital_NPI_Text,
	  CAST(CAST(chs.CreateDate AS VARCHAR(19)) AS TIMESTAMP(0)) AS Source_Create_Date_Time,
          CAST(CAST(chs.LastUpdate AS VARCHAR(19)) AS TIMESTAMP(0)) AS Source_Last_Update_Date_Time,
          chs.UpdatedBy AS Updated_By_3_4_Id,
          'C' AS Source_System_Code,
	  Current_Timestamp(0) as DW_Last_Update_Date_Time
	  FROM EDWCDM_STAGING.CardioAccess_Hospital_STG chs
	  
	  INNER JOIN  EDWCDM.CA_SERVER csrv
	  ON chs.Full_Server_NM  =csrv.Server_Name 
	  
	  LEFT JOIN  EDWCDM_Views.Ref_CA_Global_Lookup cglus
	  ON Coalesce(chs.HospCountry, ' ') = Coalesce(cglus.STS_Code_Text, ' ')
	  AND cglus.Short_name = 'ISOCountry'

          LEFT JOIN EDWCDM.CA_ADDRESS ca  
	  ON Coalesce(chs.HospAddress, ' ') = Coalesce(ca.Address_Line_1_Text, ' ')
	  AND Coalesce(chs.HospAddress2, ' ') = Coalesce(ca.Address_Line_2_Text, ' ')
	  AND Coalesce(chs.HospCity, ' ') = Coalesce(ca.City_Name, ' ')
	  AND  Coalesce(chs.HospState, ' ') = Coalesce(ca.State_Name, ' ')
	  AND Coalesce(chs.HospZip, ' ') =Coalesce(ca.Zip_Code, ' ')
	  AND  Coalesce(cglus.Lookup_Id, ' ')  =Coalesce(ca.Country_Id, ' ') 
          AND ca.County_Name is null
		  
LEFT JOIN EDWCDM.CA_HOSPITAL CH 
ON CH.Server_SK = csrv.Server_SK
AND CH.Source_Hospital_Id = chs.HospitalID
where CH.Server_SK is null and CH.Source_Hospital_Id  is null)a)b;"

export AC_ACT_SQL_STATEMENT="select 'J_CDM_ADHOC_CA_Hospital'||','||
Coalesce(cast(count(*) as varchar(20)), 0)||',' as SOURCE_STRING 
FROM EDWCDM.CA_HOSPITAL 
WHERE DW_Last_Update_Date_Time >= (SELECT MAX(Job_Start_Date_Time) as Job_Start_Date_Time FROM EDWCDM_AC.ETL_JOB_RUN where Job_Name = 'J_CDM_ADHOC_CA_Hospital')
"


