
        
TRUNCATE TABLE {{ params.param_psc_core_dataset_name }}.ECW_JuncGroupPermission ;

INSERT INTO {{ params.param_psc_core_dataset_name }}.ECW_JuncGroupPermission (ECWGroupPermissionKey, RegionKey, UserId, UserKey, GroupId, permission, deleteFlag, SourcePrimaryKeyValue, DWLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM)
SELECT  ECWGroupPermissionKey,RegionKey,UserId,UserKey,GroupId,permission,deleteFlag,SourcePrimaryKeyValue,DWLastUpdateDateTime,TRIM(source.SourceSystemCode),TRIM(source.InsertedBy),InsertedDTM,TRIM(source.ModifiedBy),ModifiedDTM
FROM {{ params.param_psc_stage_dataset_name }}.ECW_JuncGroupPermission as source;
