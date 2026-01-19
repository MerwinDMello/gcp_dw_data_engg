Select
Job_Name, Step_Num, Frequency_Text, Job_Desc, Pre_Requisite_Text,
Dependant_Job_Text, Application_Code, Command_Name
From
edwcl_staging.doclib_copy
Where Upper(Job_Name) LIKE 'PB%';