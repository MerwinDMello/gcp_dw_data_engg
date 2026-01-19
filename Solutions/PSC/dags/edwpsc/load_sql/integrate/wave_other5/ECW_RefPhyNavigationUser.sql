
        
TRUNCATE TABLE {{ params.param_psc_core_dataset_name }}.ECW_RefPhyNavigationUser ;

INSERT INTO {{ params.param_psc_core_dataset_name }}.ECW_RefPhyNavigationUser (RefPhyNavigationUserKey, User_ID, LastName, FirstName, ROLE, DefaultAccess, Security_Filter, Device)
SELECT source.RefPhyNavigationUserKey, TRIM(source.User_ID), TRIM(source.LastName), TRIM(source.FirstName), TRIM(source.ROLE), TRIM(source.DefaultAccess), TRIM(source.Security_Filter), TRIM(source.Device)
FROM {{ params.param_psc_stage_dataset_name }}.ECW_RefPhyNavigationUser as source;
