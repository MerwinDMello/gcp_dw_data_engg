export Job_Name='J_IM_Ref_AD_User_Title'
export JOBNAME='J_IM_Ref_AD_User_Title'
export col_name='AD_User_Title_Id'
export tab_name='EDWIM.Ref_AD_User_Title'

export AC_EXP_SQL_STATEMENT="select 'Ref_AD_User_Title' + ',' +
coalesce(cast(ltrim(rtrim(A.counts)) as varchar(20)),'0')+ ','  AS SOURCE_STRING FROM  
(
select count(distinct title ) as counts from 
		[IDMart].[dbo].[ActiveDirectoryUsers] where coalesce(title,'')<> '') A;" 

export AC_ACT_SQL_STATEMENT="  select 'J_IM_Ref_IM_Domain' ||','||cast(zeroifnull(A.counts) as varchar(20)) ||',' as source_string from 
(
SELECT
	COUNT(*) as counts
FROM EDWIM.Ref_AD_User_Title                

           
           )A;"