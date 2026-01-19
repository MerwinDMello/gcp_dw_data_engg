#  @@START DMEXPRESS_EXPORTED_VARIABLES



export AC_EXP_TOLERANCE_PERCENT=0
export AC_EXP_TOLERANCE_AMT=0
export Job_Name='J_IM_BoardCert'
export JOBNAME='J_IM_BoardCert'

export ODBC_EXP_DB=$ODBC_MDSTAFF_DB
export ODBC_EXP_USER=$ODBC_MDSTAFF_USER
export ODBC_EXP_PASSWORD=$ODBC_MDSTAFF_PASSWORD


export AC_EXP_SQL_STATEMENT="select 'J_IM_BoardCert' +','+cast(count(*) as varchar(20)) +',' AS SOURCE_STRING from 
(
SELECT [ID]
      ,[ProviderID]
      ,[LastName]
      ,[Degree]
      ,[FirstName]
      ,[MiddleName]
      ,[Name]
      ,[OnStaff]
      ,[Suffix]
      ,CAST([BoardName] AS VARCHAR(255)) AS BoardName
      ,[BoardAddress]
      ,[BoardAddress2]
      ,[BoardCity]
      ,[BoardState]
      ,[BoardZip]
      ,[BoardTelephone]
      ,[CertificationNumber]
      ,[CertificationStatus]
      ,[Department]
      ,[FormalName]
      ,[FormalNameWithDegree]
      ,[InUse]
      ,[Lifetime]
      ,[MOC]
      ,[NameWithDegree]
      ,cast([ReCertifiedDate] as date) as [ReCertifiedDate]
      ,cast([ReVerifyDate] as date) as [ReVerifyDate]
      ,[Specialty1]
      ,cast([DownloadDate] as datetime) as [DownloadDate]
  FROM dbo.BoardCert with (nolock)
  WHERE DownloadDate = (SELECT MAX(DownloadDate) FROM dbo.BoardCert)      
)A;"


export AC_ACT_SQL_STATEMENT="select 'J_IM_BoardCert' ||','||cast(zeroifnull(A.counts) as varchar(20)) ||',' as source_string from 
(
SELECT
	COUNT(*) as counts
 FROM EDWIM_Staging.Stag_MD_Staff_Board_Certification        
)A;"






#  @@END DMEXPRESS_EXPORTED_VARIABLES
#  End of Exported variables section
#



 