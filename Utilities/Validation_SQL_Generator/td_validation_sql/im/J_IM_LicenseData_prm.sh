#  @@START DMEXPRESS_EXPORTED_VARIABLES



export AC_EXP_TOLERANCE_PERCENT=0
export AC_EXP_TOLERANCE_AMT=0
export Job_Name='J_IM_LicenseData'
export JOBNAME='J_IM_LicenseData'

export ODBC_EXP_DB=$ODBC_MDSTAFF_DB
export ODBC_EXP_USER=$ODBC_MDSTAFF_USER
export ODBC_EXP_PASSWORD=$ODBC_MDSTAFF_PASSWORD


export AC_EXP_SQL_STATEMENT="select 'J_IM_LicenseData' +','+cast(count(*) as varchar(20)) +',' AS SOURCE_STRING from 
(
SELECT  ID
      ,ProviderID
      ,LastName
      ,Degree
      ,FirstName
      ,MiddleName
      ,Name
      ,OnStaff
      ,Suffix
      ,CAST(Status AS VARCHAR(255)) AS Status
      ,CAST(Agency AS VARCHAR(255)) AS Agency
	  ,CAST(AgencyAddress AS VARCHAR(255)) AS AgencyAddress
      ,AgencyAddress2
      ,AgencyCity
	  ,AgencyState
      ,AgencyCountry
	  ,AgencyTelephone
      ,AgencyFax   
      ,AgencyTaxID    
      ,Department
      ,Expired
      ,FieldOfLicensure
      ,FormalName
      ,FormalNameWithDegree
      ,IsPrimaryBit
      ,Issued
      ,trueIssue
      ,LicenseCountry
      ,LicenseCountryCode
      ,LicenseNumber
      ,LicenseState
      , CAST(LicenseType AS VARCHAR(255)) AS LicenseType
      ,NameWithDegree
      ,CAST(Specialty1 AS VARCHAR(255)) AS Specialty1
      ,DownloadDate
  FROM dbo.LicenseData
  WHERE DownloadDate = (SELECT MAX(DownloadDate) FROM dbo.LicenseData)    
)A;"


export AC_ACT_SQL_STATEMENT="select 'J_IM_LicenseData' ||','||cast(zeroifnull(A.counts) as varchar(20)) ||',' as source_string from 
(
SELECT
	COUNT(*) as counts
 FROM EDWIM_Staging.Stag_MD_Staff_Provider_License       
)A;"






#  @@END DMEXPRESS_EXPORTED_VARIABLES
#  End of Exported variables section
#



 