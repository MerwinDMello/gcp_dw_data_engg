SELECT 'J_CR_RO_RAD_ONC_ACTVT_PRIORITY'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING 
FROM
(Select distinct
Case When Trim(DHD.ActivityPriority)='' Then Null ELSE Trim(DHD.ActivityPriority) END as Activity_Priority_Desc,
'R' as Source_System_Code,
CURRENT_TIMESTAMP(0) as DW_Last_Update_Date_Time
from EDWCR_STAGING.stg_DimActivityTransaction DHD
)src
where src.Activity_Priority_Desc is not null