
        
TRUNCATE TABLE {{ params.param_psc_core_dataset_name }}.ECW_RefPhyNavigation ;

INSERT INTO {{ params.param_psc_core_dataset_name }}.ECW_RefPhyNavigation (User_ID, LastName, FirstName, ROLE, DefaultAccess, Security_Filter, Device, Project, Document_ID, Image_Path, URL)
SELECT TRIM(source.User_ID), TRIM(source.LastName), TRIM(source.FirstName), TRIM(source.ROLE), TRIM(source.DefaultAccess), TRIM(source.Security_Filter), TRIM(source.Device), TRIM(source.Project), TRIM(source.Document_ID), TRIM(source.Image_Path), TRIM(source.URL)
FROM {{ params.param_psc_stage_dataset_name }}.ECW_RefPhyNavigation as source;
