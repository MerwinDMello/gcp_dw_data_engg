#  @@START DMEXPRESS_EXPORTED_VARIABLES



export AC_EXP_TOLERANCE_PERCENT=0
export AC_EXP_TOLERANCE_AMT=0
export Job_Name='J_IM_Insurance'
export JOBNAME='J_IM_Insurance'

export ODBC_EXP_DB=$ODBC_MDSTAFF_DB
export ODBC_EXP_USER=$ODBC_MDSTAFF_USER
export ODBC_EXP_PASSWORD=$ODBC_MDSTAFF_PASSWORD

export AC_EXP_SQL_STATEMENT="select 'J_IM_Insurance' +','+cast(count(*) as varchar(20)) +',' AS SOURCE_STRING from 
(
SELECT [ID]
      ,CAST([ProviderID] AS VARCHAR(255)) AS [ProviderID] 
      ,[FirstName]
      ,[MiddleName]
      ,[Name]
      ,[Suffix]
	  ,[ExpiredDate]
	  ,[FormalName]
	  ,[FormalNameWithDegree]
	  ,[IssuedDate]
      ,[NameWithDegree]
      ,[PolicyNumber]
	  ,[RetroDate]
      ,[DownloadDate]
	  ,CAST([CarrierName] AS VARCHAR(255)) AS [CarrierName]
      ,CAST([CarrierAddress] AS VARCHAR(255)) AS [CarrierAddress] 
      ,CAST([CarrierAddress2] AS VARCHAR(255)) AS [CarrierAddress2] 
	  ,CarrierCity
	  ,CarrierState
      ,CarrierZip
      ,CarrierTelephone
	  ,CAST([Terms] AS VARCHAR(255)) AS TERMS
      ,CAST([Coverage] AS VARCHAR(255)) AS COVERAGE
      ,[Department]
  FROM [dbo].[Insurance] 
WHERE DownloadDate = (SELECT MAX(DownloadDate) FROM dbo.Insurance)   
)A;"


export AC_ACT_SQL_STATEMENT="select 'J_IM_Insurance' ||','||cast(zeroifnull(A.counts) as varchar(20)) ||',' as source_string from 
(
SELECT
	COUNT(*) as counts
 FROM EDWIM_Staging.Stag_MD_Staff_Provider_Insurance      
)A;"






#  @@END DMEXPRESS_EXPORTED_VARIABLES
#  End of Exported variables section
#



 