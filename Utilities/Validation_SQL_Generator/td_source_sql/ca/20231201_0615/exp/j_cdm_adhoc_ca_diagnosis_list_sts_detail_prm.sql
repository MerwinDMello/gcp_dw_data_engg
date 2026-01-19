
select '$JOBNAME' || ',' || cast(count(*) as varchar(30)) || ',' as SOURCE_STRING FROM (
SELECT DISTINCT * FROM (
	SELECT DISTINCT
	ca.Diagnosis_List_SK as Diagnosis_List_Sk,
	wrk.Server_SK as Server_SK,
	wrk.Data_Version_Code as Data_Version_Code,
      	wrk.STS_Dup_SK as STS_Dup_SK,
      	wrk.STS_Harvest_Code as STS_Harvest_Code,
     	wrk.STS_Short_Term_Text as STS_Short_Term_Text,
      	wrk.STS_Base_Term_Num as STS_Base_Term_Num,
	wrk.Source_System_Code AS Source_System_Code,
	wrk.DW_Last_Update_Date_Time as DW_Last_Update_Date_Time
	FROM(
	SELECT Distinct
	null AS Diagnosis_List_SK,
	csrv.Server_SK  AS Server_SK,
	stg.ID as Source_Diagnosis_List_Id,
	'2.3/2.5' as Data_Version_Code,
	RD.Source_STS_Dup_Id as STS_Dup_SK,
	stg.STSID250 as STS_Harvest_Code,
	stg.STSTerm250 as STS_Short_Term_Text,
	null as STS_Base_Term_Num,
          'C' AS Source_System_Code,
	  Current_Timestamp(0) as DW_Last_Update_Date_Time
	  FROM EDWCDM_STAGING.CardioAccess_DiagnosisList_STG stg
	 Left outer join EDWCDM.Ref_CA_STS_Dup RD
	 on stg.STSDup250=RD.Source_STS_Dup_Id
	  INNER JOIN  EDWCDM.CA_SERVER csrv
	  ON stg.Full_Server_NM  =csrv.Server_Name 
	where stg.STSDup250 is not null or stg.STSID250 is not null
	or stg.STSTerm250 is not null
	UNION
	  SELECT Distinct
	  null AS Diagnosis_List_SK,
	csrv.Server_SK  AS Server_SK,
	stg.ID as Source_Diagnosis_List_Id,
	'3.0' as Data_Version_Code,
	RD.Source_STS_Dup_Id as STS_Dup_SK,
	stg.STSID30 as STS_Harvest_Code,
	stg.STSTerm30 as STS_Short_Term_Text,
	stg.STSBaseTerm as STS_Base_Term_Num,
          'C' AS Source_System_Code,
	  Current_Timestamp(0) as DW_Last_Update_Date_Time
	  FROM EDWCDM_STAGING.CardioAccess_DiagnosisList_STG stg
	 Left outer join EDWCDM.Ref_CA_STS_Dup RD
	on stg.STSDup30=RD.Source_STS_Dup_Id
	  INNER JOIN  EDWCDM.CA_SERVER csrv
	  ON stg.Full_Server_NM  =csrv.Server_Name 
	where stg.STSDup30 is not null or stg.STSID30 is not null
	or stg.STSTerm30 is not null and stg.STSBaseTerm is not null
	UNION
	  SELECT Distinct
	  null AS Diagnosis_List_SK,
	csrv.Server_SK  AS Server_SK,
	stg.ID as Source_Diagnosis_List_Id,
	'3.22' as Data_Version_Code,
	RD.Source_STS_Dup_Id as STS_Dup_SK,
	stg.STSID32 as STS_Harvest_Code,
	stg.STSTerm32 as STS_Short_Term_Text,
	stg.STSBaseTerm32 as STS_Base_Term_Num,
          'C' AS Source_System_Code,
	  Current_Timestamp(0) as DW_Last_Update_Date_Time
	  FROM EDWCDM_STAGING.CardioAccess_DiagnosisList_STG stg
	 Left outer join EDWCDM.Ref_CA_STS_Dup RD
	on stg.STSDup32=RD.Source_STS_Dup_Id
	  INNER JOIN  EDWCDM.CA_SERVER csrv
	  ON stg.Full_Server_NM  =csrv.Server_Name 
	where stg.STSDup32 is not null or stg.STSID32 is not null
	or stg.STSTerm32 is not null and stg.STSBaseTerm32 is not null
	UNION
	  SELECT Distinct
	  null AS Diagnosis_List_SK,
	csrv.Server_SK  AS Server_SK,
	stg.ID as Source_Diagnosis_List_Id,
	'3.3' as Data_Version_Code,
	RD.Source_STS_Dup_Id as STS_Dup_SK,
	stg.STSID33 as STS_Harvest_Code,
	stg.STSTerm33 as STS_Short_Term_Text,
	stg.STSBaseTerm33 as STS_Base_Term_Num,
          'C' AS Source_System_Code,
	  Current_Timestamp(0) as DW_Last_Update_Date_Time
	  FROM EDWCDM_STAGING.CardioAccess_DiagnosisList_STG stg
	 Left outer join EDWCDM.Ref_CA_STS_Dup RD
	on stg.STSDup33=RD.Source_STS_Dup_Id
	  INNER JOIN  EDWCDM.CA_SERVER csrv
	  ON stg.Full_Server_NM  =csrv.Server_Name
	where stg.STSDup33 is not null or stg.STSID33 is not null
	or stg.STSTerm33 is not null and stg.STSBaseTerm33 is not null)wrk 
	Inner Join EDWCDM.CA_Diagnosis_List CA
	ON ca.Source_Diagnosis_List_ID=wrk.Source_Diagnosis_List_Id
	and ca.Server_SK = wrk.Server_SK
	LEFT JOIN EDWCDM.CA_Diagnosis_List_STS_Detail CH 
	on CH.Diagnosis_List_SK = ca.Diagnosis_List_SK
	where CH.Diagnosis_List_SK is null)a)b;