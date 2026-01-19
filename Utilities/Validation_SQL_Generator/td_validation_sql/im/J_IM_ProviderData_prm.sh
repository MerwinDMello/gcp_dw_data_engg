#  @@START DMEXPRESS_EXPORTED_VARIABLES



export AC_EXP_TOLERANCE_PERCENT=0
export AC_EXP_TOLERANCE_AMT=0
export Job_Name='J_IM_ProviderData'
export JOBNAME='J_IM_ProviderData'

export ODBC_EXP_DB=$ODBC_MDSTAFF_DB
export ODBC_EXP_USER=$ODBC_MDSTAFF_USER
export ODBC_EXP_PASSWORD=$ODBC_MDSTAFF_PASSWORD

export AC_EXP_SQL_STATEMENT="select 'J_IM_ProviderData' +','+cast(count(*) as varchar(20)) +',' AS SOURCE_STRING from 
(
SELECT  [ID]
      ,CAST([ProviderID] AS VARCHAR(255)) AS ProviderID
      ,[LastName]
      ,[Degree]
      ,[FirstName]
      ,[MiddleName]
      ,[Name]
      ,[OnStaff]
      ,[Suffix]
      ,[AdmittingPrivileges]
      ,[Age]
      ,[Attending]
      ,[BillingAddress]
      ,[MailingAddressFax]
      ,[BillingAddress2]
      ,CAST([BillingAddressBlock] AS VARCHAR(255)) AS BillingAddressBlock
      ,[BillingAddressCity]
      ,[BillingAddressFax]
      ,[BillingAddressMedicaidNumber]
      ,CAST([BillingAddressMedicalGroup] AS VARCHAR(255)) AS BillingAddressMedicalGroup
      ,[BillingAddressMedicalGroupId]
      ,[BillingAddressMedicareNumber]
      ,[BillingAddressNPI]
      ,[BillingAddressState]
      ,[BillingAddressTelephone]
      ,[BillingAddressZip]
      ,[BirthDate]
      ,[BirthDateText]
      ,[BirthDay]
      ,[BirthMonth]
      ,[BirthPlace]
      ,[BirthYear]
      ,[Category]
      ,[CategoryCode]
      ,[Citizen]
      ,[ContractDate]
      ,[ContractExpired]
      ,[ContractStatusCode]
      ,[CorporateStatus]
      ,[CorporateStatusCode]
      ,[CredentialingComplete]
      ,[Degree1]
      ,[Degree2]
      ,[Degree3]
      ,[Department1]
      ,[Department2]
      ,[ElectronicSignature]
      ,[ElectronicSignatureBit]
      ,[Ethnicity]
      ,[Facility]
      ,[Gender]
      ,[IDNumber]
      ,[Language1]
      ,[Language2]
      ,[Language3]
      ,[Language4]
      ,[Language5]
      ,[MailingAddress]
      ,[MailingAddress2]
      ,[MailingAddressCity]
      ,[MailingAddressMedicaidNumber]
      ,CAST([MailingAddressMedicalGroup] AS VARCHAR(255)) AS MailingAddressMedicalGroup
      ,[MailingAddressMedicareNumber]
      ,[MailingAddressNPI]
      ,[MailingAddressState]
      ,[MailingAddressTelephone]
      ,[MailingAddressZip]
      ,[MedicaidNumber]
      ,[NPI]
      ,[Office1Address]
      ,[Office1Address2]
      ,[Office1AddressCity]
      ,[Office1AddressFax]
      ,CAST([Office1AddressMedicalGroup] AS VARCHAR(255)) AS Office1AddressMedicalGroup
      ,[Office1AddressState]
      ,[Office1AddressTelephone]
      ,[Office1AddressZip]
      ,[Office1MedicaidNumber]
      ,[Office1MedicareNumber]
      ,[Office1NPI]
      ,[PCP]
      ,[PhysicianOrdering]
      ,[Salutation]
      ,[Specialty]
      ,[Specialty2]
      ,[Specialty2Code]
      ,[Specialty3]
      ,[Specialty3Code]
      ,[Specialty4]
      ,[Specialty4Code]
      ,[SpecialtyCode]
      ,[StaffType]
      ,[StaffTypeCode]
      ,[Title]
      ,[UPIN]
      ,[Status]
      ,[StatusCode]
      ,[Archived]
      ,CAST([Email] AS VARCHAR(255)) AS Email
      ,CAST([CellPhone] AS VARCHAR(255)) AS CellPhone
      ,[OtherFacilityID]
      ,[HomeAddressTelephone]
      ,[Pager]
      ,[Division]
      ,[DownloadDate]
  FROM dbo.ProviderData as t1 with (nolock)
 WHERE DownloadDate = (SELECT MAX(DownloadDate) FROM dbo.ProviderData)        
)A;"


export AC_ACT_SQL_STATEMENT="select 'J_IM_ProviderData' ||','||cast(zeroifnull(A.counts) as varchar(20)) ||',' as source_string from 
(
SELECT
	COUNT(*) as counts
 FROM EDWIM_Staging.Stag_MD_Staff_Provider_Detail        
)A;"






#  @@END DMEXPRESS_EXPORTED_VARIABLES
#  End of Exported variables section
#



 