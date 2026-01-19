export JOBNAME='J_CDM_ADHOC_CA_CONTACT'

export AC_EXP_SQL_STATEMENT="
select '$JOBNAME' || ',' || cast(count(*) as varchar(30)) || ',' as SOURCE_STRING FROM (
SELECT DISTINCT * FROM (
 SELECT Distinct
	  null AS Contact_SK,
	  A.Contact_Type_SK as Contact_Type_SK,
	  addr.Address_Sk as Address_Sk,
	  ser.Server_SK as Server_SK,
         stg.ContactID as Source_Contact_Id,
         stg.FirstName as Contact_First_Name,
         stg.MiddleName as Contact_Middle_Name,
         stg.LastName as Contact_Last_Name,
         stg.Suffix as Contact_Suffix_Name,
         stg.EmailName as Email_Address_Text,
         stg.CompanyName as Company_Name,
         stg.Notes as Note_Text,
	  CAST(CAST(stg.DateEntered AS VARCHAR(19)) AS TIMESTAMP(0)) as Contact_Effective_From_Date,
	  CAST(CAST(stg.Inactive AS VARCHAR(19)) AS TIMESTAMP(0)) as Contact_Effective_To_Date,
	  CAST(CAST(stg.CreateDate AS VARCHAR(19)) AS TIMESTAMP(0)) as Source_Create_Date_Time,
	  CAST(CAST(stg.LastUpdate AS VARCHAR(19)) AS TIMESTAMP(0)) as Source_Last_Update_Date_Time,
          stg.UpdatedBy AS Updated_By_3_4_Id,
          'C' AS Source_System_Code,
	  Current_Timestamp(0) as DW_Last_Update_Date_Time
	  FROM EDWCDM_STAGING.CardioAccess_Contacts_STG stg

	 left outer join 
	 ( Select Contact_Type_SK, Source_Contact_Type_Id ,Server_Name From
 	EDWCDM.CA_Contact_Type  C
 	Inner Join EDWCDM.CA_Server S on
	 C.Server_SK=S.Server_SK
 	  ) A
 	on Stg.ContactType = A.Source_Contact_Type_Id
 	and Stg.Full_Server_NM=A.Server_Name

	LEFT JOIN  EDWCDM_Views.Ref_CA_Global_Lookup cglus
	ON stg.Country = cglus.STS_Code_Text
	AND cglus.Short_name = 'ISOCountry'

	Left join EDWCDM.CA_Address addr
	on stg.Address = addr.Address_Line_1_Text and
	stg.Address2 = addr.Address_Line_2_Text and
	stg.Address3 = addr.Address_Line_3_Text and
	stg.City = addr.City_Name and
	stg.StateorProvince = addr.State_Name and
	stg.Postalcode = addr.Zip_Code and
	stg.County = addr.County_Name and
	cglus.Lookup_Id = addr.Country_Id

	Inner Join EDWCDM.CA_Server ser
	on stg.Full_Server_NM=ser.Server_Name
	
	LEFT JOIN EDWCDM.CA_CONTACT CH 
	ON CH.Server_SK = ser.Server_SK
	AND CH.Source_Contact_Id = stg.ContactID
	where CH.Server_SK is null and CH.Source_Contact_Id is null)a)b;"

export AC_ACT_SQL_STATEMENT="select 'J_CDM_ADHOC_CA_CONTACT'||','||
Coalesce(cast(count(*) as varchar(20)), 0)||',' as SOURCE_STRING 
FROM EDWCDM.CA_CONTACT 
WHERE DW_Last_Update_Date_Time >= (SELECT MAX(Job_Start_Date_Time) as Job_Start_Date_Time FROM EDWCDM_AC.ETL_JOB_RUN where Job_Name = 'J_CDM_ADHOC_CA_CONTACT')
"