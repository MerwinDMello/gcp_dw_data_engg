
with SC AS (
SELECT EMR_Date.*, (EDWCR_Date_Driver - INTERVAL '90' DAY) AS HospEMR_Date_Driver FROM
(SELECT 
info.Cancer_Patient_Driver_SK, 
info.Network_Mnemonic_CS,
info.Patient_Market_URN_Text,
info.Medical_Record_Num,
info.EMPI_Text,
info.Coid,
info.DateSource,
MIN(info.date_driver) AS EDWCR_Date_Driver
FROM (
SELECT 
Cancer_Patient_Driver_SK, 
Network_Mnemonic_CS,
Patient_Market_URN_Text,  
MIN(DateSourceOrder) AS MinDateSource
FROM 
(
SELECT t2.Cancer_Patient_Driver_SK, t2.Network_Mnemonic_CS, t2.Patient_Market_URN_Text, t2.Medical_Record_Num, t2.EMPI_Text, 'Patient_ID' AS DateSource, 3 AS DateSourceOrder, MIN(User_Action_Date_Time) AS date_driver FROM EDWCR_Base_Views.Cancer_Patient_Id_Output t1
JOIN Cancer_Patient_Driver t2
ON t1.Patient_DW_ID = t2.CP_Patient_ID
WHERE User_Action_Desc = 'Confirm'
AND Message_Flag_Code <> 'RAD'
GROUP BY 1,2,3,4,5
UNION
SELECT t1.Cancer_Patient_Driver_SK, t1.Network_Mnemonic_CS, t1.Patient_Market_URN_Text, t1.Medical_Record_Num, t1.EMPI_Text,'Nav_Diagnosis' AS DateSource, 2 AS DateSourceOrder, MIN(diagnosis_date) AS date_driver FROM EDWCR_Base_Views.Cancer_Patient_Driver t1
JOIN CN_Patient t2
ON t1.CN_Patient_Id = t2.Nav_Patient_Id
JOIN CN_Patient_Diagnosis t3
ON t2.Nav_Patient_Id = t3.Nav_Patient_ID
WHERE diagnosis_date > '1900-01-01'
GROUP BY 1,2,3,4,5
UNION
SELECT t1.Cancer_Patient_Driver_SK, t1.Network_Mnemonic_CS, t1.Patient_Market_URN_Text, t1.Medical_Record_Num, t1.EMPI_Text, 'Nav_Created' AS DateSource, 4 AS DateSourceOrder, MIN(Nav_Create_Date) AS date_driver FROM EDWCR_Base_Views.Cancer_Patient_Driver t1
JOIN CN_Patient t2
ON t1.CN_Patient_Id = t2.Nav_Patient_Id
WHERE Nav_Create_date > '1900-01-01'
GROUP BY 1,2,3,4,5,6
UNION
SELECT  t1.Cancer_Patient_Driver_SK, t1.Network_Mnemonic_CS, t1.Patient_Market_URN_Text, t1.Medical_Record_Num, t1.EMPI_Text, 'CR_Diagnosis' AS DateSource, 1 AS DateSourceOrder, MIN(diagnosis_date) AS date_driver FROM EDWCR_Base_Views.Cancer_Patient_Driver t1
JOIN CR_Patient_Diagnosis_Detail t2
ON t1.CR_Patient_ID = t2.CR_Patient_Id AND diagnosis_date IS NOT NULL
WHERE diagnosis_date > '1900-01-01'
GROUP BY 1,2,3,4,5,6
UNION
SELECT  t1.Cancer_Patient_Driver_SK, t1.Network_Mnemonic_CS, t1.Patient_Market_URN_Text, t1.Medical_Record_Num, t1.EMPI_Text, 'CR_Admission' AS DateSource, 5 AS DateSourceOrder, MIN(admission_date) AS date_driver FROM EDWCR_Base_Views.Cancer_Patient_Driver t1
JOIN CR_Patient_Tumor t2
ON t1.CR_Patient_ID = t2.CR_Patient_Id AND admission_date IS NOT NULL
WHERE admission_date > '1900-01-01'
GROUP BY 1,2,3,4,5,6) dates
GROUP BY 1,2,3
) date_source
------ Date Driver ------
JOIN (
SELECT t2.Cancer_Patient_Driver_SK, t2.Network_Mnemonic_CS, t2.Patient_Market_URN_Text, t2.Medical_Record_Num, t2.EMPI_Text, t2.Coid, 'Patient_ID' AS DateSource, 3 AS DateSourceOrder, MIN(User_Action_Date_Time) AS date_driver FROM EDWCR_Base_Views.Cancer_Patient_Id_Output t1
JOIN Cancer_Patient_Driver t2
ON t1.Patient_DW_ID = t2.CP_Patient_ID
WHERE User_Action_Desc = 'Confirm'
AND Message_Flag_Code <> 'RAD'
GROUP BY 1,2,3,4,5,6
UNION
SELECT t1.Cancer_Patient_Driver_SK, t1.Network_Mnemonic_CS, t1.Patient_Market_URN_Text, t1.Medical_Record_Num, t1.EMPI_Text, t1.Coid, 'Nav_Diagnosis' AS DateSource, 2 AS DateSourceOrder, MIN(diagnosis_date) AS date_driver FROM EDWCR_Base_Views.Cancer_Patient_Driver t1
JOIN CN_Patient t2
ON t1.CN_Patient_Id = t2.Nav_Patient_Id
JOIN CN_Patient_Diagnosis t3
ON t2.Nav_Patient_Id = t3.Nav_Patient_ID
WHERE diagnosis_date > '1900-01-01'
GROUP BY 1,2,3,4,5,6
UNION
SELECT t1.Cancer_Patient_Driver_SK, t1.Network_Mnemonic_CS, t1.Patient_Market_URN_Text, t1.Medical_Record_Num, t1.EMPI_Text, t1.Coid, 'Nav_Created' AS DateSource, 4 AS DateSourceOrder, MIN(Nav_Create_Date) AS date_driver FROM EDWCR_Base_Views.Cancer_Patient_Driver t1
JOIN CN_Patient t2
ON t1.CN_Patient_Id = t2.Nav_Patient_Id
WHERE Nav_Create_date > '1900-01-01'
GROUP BY 1,2,3,4,5,6
UNION
SELECT  t1.Cancer_Patient_Driver_SK, t1.Network_Mnemonic_CS, t1.Patient_Market_URN_Text, t1.Medical_Record_Num, t1.EMPI_Text, t1.Coid, 'CR_Diagnosis' AS DateSource, 1 AS DateSourceOrder, MIN(diagnosis_date) AS date_driver FROM EDWCR_Base_Views.Cancer_Patient_Driver t1
JOIN CR_Patient_Diagnosis_Detail t2
ON t1.CR_Patient_ID = t2.CR_Patient_Id AND diagnosis_date IS NOT NULL
WHERE diagnosis_date > '1900-01-01'
GROUP BY 1,2,3,4,5,6
UNION
SELECT  t1.Cancer_Patient_Driver_SK, t1.Network_Mnemonic_CS, t1.Patient_Market_URN_Text, t1.Medical_Record_Num, t1.EMPI_Text, t1.Coid, 'CR_Admission' AS DateSource, 5 AS DateSourceOrder, MIN(admission_date) AS date_driver FROM EDWCR_Base_Views.Cancer_Patient_Driver t1
JOIN CR_Patient_Tumor t2
ON t1.CR_Patient_ID = t2.CR_Patient_Id AND admission_date IS NOT NULL
WHERE admission_date > '1900-01-01'
GROUP BY 1,2,3,4,5,6) info
ON date_source.Cancer_Patient_Driver_SK = info.Cancer_Patient_Driver_SK AND date_source.MinDateSource = info.DateSourceOrder
GROUP BY info.Cancer_Patient_Driver_SK, 
info.Network_Mnemonic_CS,
info.Patient_Market_URN_Text,
info.Medical_Record_Num,
info.EMPI_Text,
info.Coid,
info.DateSource) EMR_Date
)
SELECT 'J_CR_Patient_Date_Driver'||','|| cast(count(*) as varchar(20))||',	' as SOURCE_STRING 
FROM
(
SELECT 
Cancer_Patient_Driver_SK,
Patient_DW_Id,
EDWCR_Date_Driver AS Cancer_Diagnosis_Date,
HospEMR_Date_Driver AS Cancer_Diagnosis_90_Day_Prior_Date,
CURRENT_TIMESTAMP(0) AS DW_Last_Update_Date_Time
FROM (
	SELECT 
	FE.Patient_DW_Id,
	FE.Patient_Market_URN, 
	FE.Medical_Record_Num,
	SC.Cancer_Patient_Driver_Sk,
	FE.Admission_Date_Time,
	SC.EDWCR_Date_Driver,
	SC.HospEMR_Date_Driver
	 
	FROM
	EDWCR_Base_Views.Fact_Encounter FE
		
	INNER JOIN SC
	
	ON FE.Medical_Record_Num = SC.Medical_Record_Num
	AND FE.Coid = SC.Coid
	
	WHERE Admission_Date_Time >= HospEMR_Date_Driver
)a
union all
SELECT 
Cancer_Patient_Driver_SK,
Patient_DW_Id,
EDWCR_Date_Driver AS Cancer_Diagnosis_Date,
HospEMR_Date_Driver AS Cancer_Diagnosis_90_Day_Prior_Date,
CURRENT_TIMESTAMP(0) AS DW_Last_Update_Date_Time
FROM (
	SELECT
	FE.Patient_DW_Id,
	FE.Patient_Market_URN, 
	FE.Medical_Record_Num,
	SC.Cancer_Patient_Driver_SK,
	FE.Admission_Date_Time,
	SC.EDWCR_Date_Driver,
	SC.HospEMR_Date_Driver
	
	FROM
	EDWCR_Base_Views.Fact_Encounter FE
		
	INNER JOIN EDW_PUB_VIEWS.Clinical_Facility CF
	ON FE.Coid = CF.Coid
	AND CF.Facility_Active_Ind = 'Y'
		
	INNER JOIN SC
	ON FE.Patient_Market_URN = SC.Patient_Market_URN_Text
	AND CF.Network_Mnemonic_CS = SC.Network_Mnemonic_CS
		
	WHERE Admission_Date_Time >= HospEMR_Date_Driver
)b
WHERE b.Patient_DW_ID + b.Cancer_Patient_Driver_SK NOT IN 
	(SELECT Patient_DW_Id + Cancer_Patient_Driver_Sk
		FROM (
			SELECT 
			FE.Patient_DW_Id,
			FE.Patient_Market_URN, 
			FE.Medical_Record_Num,
			SC.Cancer_Patient_Driver_Sk,
			FE.Admission_Date_Time,
			SC.EDWCR_Date_Driver,
			SC.HospEMR_Date_Driver
			 
			FROM
			EDWCR_Base_Views.Fact_Encounter FE
				
			INNER JOIN SC
			
			ON FE.Medical_Record_Num = SC.Medical_Record_Num
			AND FE.Coid = SC.Coid
			
			WHERE Admission_Date_Time >= HospEMR_Date_Driver
		)a
	)
)A;