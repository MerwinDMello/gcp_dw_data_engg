export JOBNAME='J_CDM_ADHOC_CA_Diagnosis_List_Detail'

export AC_EXP_SQL_STATEMENT="
select '$JOBNAME' || ',' || cast(count(*) as varchar(30)) || ',' as SOURCE_STRING FROM (
	SELECT DISTINCT * FROM (
	SELECT Distinct
	ca.Diagnosis_List_SK AS Diagnosis_List_SK,
	wrk.Server_SK as Server_SK,
        wrk.Diagnosis_List_Detail_Measure_Id AS Diagnosis_List_Detail_Measure_Id,
	wrk.Diagnosis_List_Detail_Measure_Value_Text AS Diagnosis_List_Detail_Measure_Value_Text,
	wrk.Diagnosis_List_Detail_Measure_Value_Num AS Diagnosis_List_Detail_Measure_Value_Num,
        wrk.Source_System_Code AS Source_System_Code,
	wrk.DW_Last_Update_Date_Time as DW_Last_Update_Date_Time
	FROM (
	SELECT Distinct
	null AS Diagnosis_List_SK,
	ser.Server_SK as Server_SK,
	stg.ID as Source_Diagnosis_List_Id,
        '226' AS Diagnosis_List_Detail_Measure_Id,
	cast(NULL as varchar(255)) AS Diagnosis_List_Detail_Measure_Value_Text,
	cast(stg.KingdomCode as varchar(255)) AS Diagnosis_List_Detail_Measure_Value_Num,
        'C' AS Source_System_Code,
	Current_Timestamp(0) as DW_Last_Update_Date_Time
	FROM EDWCDM_STAGING.CardioAccess_DiagnosisList_STG stg

	Inner Join EDWCDM.CA_Server ser
	on stg.Full_Server_NM=ser.Server_Name
	where stg.KingdomCode is not null

	UNION

	SELECT Distinct
	null AS Diagnosis_List_SK,
	ser.Server_SK as Server_SK,
	stg.ID as Source_Diagnosis_List_Id,
        '227' AS Diagnosis_List_Detail_Measure_Id,
	cast(stg.Kingdom as varchar(255)) AS Diagnosis_List_Detail_Measure_Value_Text,
	cast(NULL as varchar(255)) AS Diagnosis_List_Detail_Measure_Value_Num,
        'C' AS Source_System_Code,
	Current_Timestamp(0) as DW_Last_Update_Date_Time
	FROM EDWCDM_STAGING.CardioAccess_DiagnosisList_STG stg

	Inner Join EDWCDM.CA_Server ser
	on stg.Full_Server_NM=ser.Server_Name
	where stg.Kingdom is not null

	UNION

	SELECT Distinct
	null AS Diagnosis_List_SK,
	ser.Server_SK as Server_SK,
	stg.ID as Source_Diagnosis_List_Id,
        '228' AS Diagnosis_List_Detail_Measure_Id,
	cast(stg.Phylum as varchar(255)) AS Diagnosis_List_Detail_Measure_Value_Text,
	cast(NULL as varchar(255)) AS Diagnosis_List_Detail_Measure_Value_Num,
        'C' AS Source_System_Code,
	Current_Timestamp(0) as DW_Last_Update_Date_Time
	FROM EDWCDM_STAGING.CardioAccess_DiagnosisList_STG stg

	Inner Join EDWCDM.CA_Server ser
	on stg.Full_Server_NM=ser.Server_Name
	Where stg.Phylum is not null
	
	UNION

	SELECT Distinct
	null AS Diagnosis_List_SK,
	ser.Server_SK as Server_SK,
	stg.ID as Source_Diagnosis_List_Id,
        '229' AS Diagnosis_List_Detail_Measure_Id,
	cast(stg.IPCCC as varchar(255)) AS Diagnosis_List_Detail_Measure_Value_Text,
	cast(NULL as varchar(255)) AS Diagnosis_List_Detail_Measure_Value_Num,
        'C' AS Source_System_Code,
	Current_Timestamp(0) as DW_Last_Update_Date_Time
	FROM EDWCDM_STAGING.CardioAccess_DiagnosisList_STG stg

	Inner Join EDWCDM.CA_Server ser
	on stg.Full_Server_NM=ser.Server_Name
	Where stg.IPCCC is not null

	UNION

	SELECT Distinct
	null AS Diagnosis_List_SK,
	ser.Server_SK as Server_SK,
	stg.ID as Source_Diagnosis_List_Id,
        '230' AS Diagnosis_List_Detail_Measure_Id,
	cast(NULL as varchar(255)) AS Diagnosis_List_Detail_Measure_Value_Text,
	cast(stg.FundDx as varchar(255)) AS Diagnosis_List_Detail_Measure_Value_Num,
        'C' AS Source_System_Code,
	Current_Timestamp(0) as DW_Last_Update_Date_Time
	FROM EDWCDM_STAGING.CardioAccess_DiagnosisList_STG stg

	Inner Join EDWCDM.CA_Server ser
	on stg.Full_Server_NM=ser.Server_Name
	where stg.FundDx is not null

	UNION

	SELECT Distinct
	null AS Diagnosis_List_SK,
	ser.Server_SK as Server_SK,
	stg.ID as Source_Diagnosis_List_Id,
        '231' AS Diagnosis_List_Detail_Measure_Id,
	cast(NULL as varchar(255)) AS Diagnosis_List_Detail_Measure_Value_Text,
	cast(stg.PC4FundDx as varchar(255)) AS Diagnosis_List_Detail_Measure_Value_Num,
        'C' AS Source_System_Code,
	Current_Timestamp(0) as DW_Last_Update_Date_Time
	FROM EDWCDM_STAGING.CardioAccess_DiagnosisList_STG stg

	Inner Join EDWCDM.CA_Server ser
	on stg.Full_Server_NM=ser.Server_Name
	where stg.PC4FundDx is not null

	UNION

	SELECT Distinct
	null AS Diagnosis_List_SK,
	ser.Server_SK as Server_SK,
	stg.ID as Source_Diagnosis_List_Id,
        '232' AS Diagnosis_List_Detail_Measure_Id,
	cast(NULL as varchar(255)) AS Diagnosis_List_Detail_Measure_Value_Text,
	cast(stg.FundDx32 as varchar(255)) AS Diagnosis_List_Detail_Measure_Value_Num,
        'C' AS Source_System_Code,
	Current_Timestamp(0) as DW_Last_Update_Date_Time
	FROM EDWCDM_STAGING.CardioAccess_DiagnosisList_STG stg

	Inner Join EDWCDM.CA_Server ser
	on stg.Full_Server_NM=ser.Server_Name
	where stg.FundDx32 is not null

	UNION

	SELECT Distinct
	null AS Diagnosis_List_SK,
	ser.Server_SK as Server_SK,
	stg.ID as Source_Diagnosis_List_Id,
        '233' AS Diagnosis_List_Detail_Measure_Id,
	cast(NULL as varchar(255)) AS Diagnosis_List_Detail_Measure_Value_Text,
	cast(stg.FundDx33 as varchar(255)) AS Diagnosis_List_Detail_Measure_Value_Num,
        'C' AS Source_System_Code,
	Current_Timestamp(0) as DW_Last_Update_Date_Time
	FROM EDWCDM_STAGING.CardioAccess_DiagnosisList_STG stg

	Inner Join EDWCDM.CA_Server ser
	on stg.Full_Server_NM=ser.Server_Name
	Where stg.FundDx33 is not null)wrk
	
		Inner Join EDWCDM.CA_Diagnosis_List CA
	ON ca.Source_Diagnosis_List_ID=wrk.Source_Diagnosis_List_Id
	and ca.Server_SK = wrk.Server_SK
	
	LEFT JOIN EDWCDM.CA_Diagnosis_List_Detail CH 
	on CH.Diagnosis_List_SK = ca.Diagnosis_List_SK
	where CH.Diagnosis_List_SK is null)a)b;"

export AC_ACT_SQL_STATEMENT="select 'J_CDM_ADHOC_CA_Diagnosis_List_Detail'||','||
Coalesce(cast(count(*) as varchar(20)), 0)||',' as SOURCE_STRING 
FROM EDWCDM.CA_Diagnosis_List_Detail 
WHERE DW_Last_Update_Date_Time >= (SELECT MAX(Job_Start_Date_Time) as Job_Start_Date_Time FROM EDWCDM_AC.ETL_JOB_RUN where Job_Name = 'J_CDM_ADHOC_CA_Diagnosis_List_Detail')
"