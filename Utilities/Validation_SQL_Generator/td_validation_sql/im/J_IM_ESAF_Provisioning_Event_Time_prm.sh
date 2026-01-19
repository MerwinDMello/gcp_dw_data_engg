#  @@START DMEXPRESS_EXPORTED_VARIABLES

export AC_EXP_TOLERANCE_PERCENT=0
export AC_EXP_TOLERANCE_AMT=0
export Job_Name='J_IM_ESAF_Provisioning_Event_Time'
export JOBNAME='J_IM_ESAF_Provisioning_Event_Time'

export ODBC_EXP_DB='ODBC_ESAF_SQL_DB'
export ODBC_EXP_USER='DMX_eSAF'
export ODBC_EXP_PASSWORD='@bby_3lm0'


export AC_EXP_SQL_STATEMENT="select 'J_IM_ESAF_Provisioning_Event_Time' + ',' +
coalesce(cast(ltrim(rtrim(A.SRC_COUNT)) as varchar(20)),'0')+ ','  AS SOURCE_STRING FROM  
(SELECT 
count(*) as SRC_COUNT

FROM(
select  
AuditEventKey,
TargetUID, 
Target2,
Operation,
EventTime,
Source_System_Code,
DW_Last_Update_Date_Time
from 
(
select  
AuditEventKey,
TargetUID, 
Target2,
Operation,
(CASE WHEN EventTime >= '1/22/2022' THEN convert(datetime,switchoffset(convert(datetimeoffset, EventTime),datename(tzoffset, sysdatetimeoffset()))) 
ELSE EventTime END)  AS EventTime,
Source_System_Code,
DW_Last_Update_Date_Time

from 
(
select  
MAX(AuditEventKey)  AuditEventKey,
UPPER(SUBSTRING(Target, 
        (CASE WHEN CHARINDEX('CN=', [Target]) > 0 THEN CHARINDEX('CN=', [Target]) + 3 ELSE 1 END),
            (CASE WHEN CHARINDEX('CN=', [Target]) > 0 THEN 7 ELSE 8 END)) ) TargetUID, 
CAST(Target2 as varchar(255)) Target2,
CASE
        WHEN Operation = 8
THEN 'Create'
        WHEN Operation = 9
THEN 'Modify'
End as Operation,
max(EventTime) EventTime,
'E' Source_System_Code,
CURRENT_TIMESTAMP DW_Last_Update_Date_Time

from [Auditing].[dbo].[VergenceAuditEvent]  WITH (NOLOCK)

where Operation in ('8','9') 
and SUBSTRING(Target, 
        (CASE WHEN CHARINDEX('CN=', [Target]) > 0 THEN CHARINDEX('CN=', [Target]) + 3 ELSE 1 END),
            (CASE WHEN CHARINDEX('CN=', [Target]) > 0 THEN 7 ELSE 8 END)) <> ''
and Step = 10 
and EventResult = 1 

group by    
 UPPER(SUBSTRING(Target, 
        (CASE WHEN CHARINDEX('CN=', [Target]) > 0 THEN CHARINDEX('CN=', [Target]) + 3 ELSE 1 END),
            (CASE WHEN CHARINDEX('CN=', [Target]) > 0 THEN 7 ELSE 8 END)) ) , 
 Target2,
 Operation 

having MAX(EventTime) > dateadd(day,-90, getdate())

UNION

select  
MAX(vae2.AuditEventKey) AuditEventKey,
UPPER(SUBSTRING(vae.Target, 
        (CASE WHEN CHARINDEX('CN=', vae.[Target]) > 0 THEN CHARINDEX('CN=', vae.[Target]) + 3 ELSE 1 END),
            (CASE WHEN CHARINDEX('CN=', vae.[Target]) > 0 THEN 7 ELSE 8 END)) ) TargetUID, 
CAST(vae.Target2 as varchar(255)) Target2,
CASE
        WHEN vae.Operation = 8
THEN 'Create'
        WHEN vae.Operation = 9
THEN 'Modify'
End as Operation,
max(vae2.EventTime) EventTime,
'E' Source_System_Code,
CURRENT_TIMESTAMP DW_Last_Update_Date_Time
from [Auditing].[dbo].[VergenceAuditEvent] vae WITH (NOLOCK)
join [Auditing].[dbo].[VergenceAuditEvent] vae2 WITH (NOLOCK)
on vae.AudittransactionID = vae2.AudittransactionID
and vae.Operation = vae2.Operation
and vae2.step = 2
where vae.Operation in ('8','9') 
and SUBSTRING(vae.Target, 
        (CASE WHEN CHARINDEX('CN=', vae.[Target]) > 0 THEN CHARINDEX('CN=', vae.[Target]) + 3 ELSE 1 END),
            (CASE WHEN CHARINDEX('CN=', vae.[Target]) > 0 THEN 7 ELSE 8 END)) <> ''
and vae.Step = 1 
and UPPER(CAST(vae.Target2 as varchar(255)))  = 'NETACCESS'
group by    
 UPPER(SUBSTRING(vae.Target, 
        (CASE WHEN CHARINDEX('CN=', vae.[Target]) > 0 THEN CHARINDEX('CN=', vae.[Target]) + 3 ELSE 1 END),
            (CASE WHEN CHARINDEX('CN=', vae.[Target]) > 0 THEN 7 ELSE 8 END)) ) , 
 vae.Target2,
 vae.Operation 
having max(vae2.EventTime) > dateadd(day,-90, getdate())
) e
) t
) S	) a;" 

export AC_ACT_SQL_STATEMENT="select 'J_IM_ESAF_Provisioning_Event_Time' ||','||cast(A.counts as varchar(20)) ||',' as source_string from 
(
SELECT
	COUNT(*) as counts
	FROM EDWIM.eSAF_Provisioning_Event_Time                
)A;"





#  @@END DMEXPRESS_EXPORTED_VARIABLES
#  End of Exported variables section
#



 