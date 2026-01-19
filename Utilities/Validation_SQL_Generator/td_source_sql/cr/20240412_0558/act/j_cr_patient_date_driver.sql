select 'J_CR_Patient_Date_Driver'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING 
FROM edwcr.CR_Patient_Date_Driver;