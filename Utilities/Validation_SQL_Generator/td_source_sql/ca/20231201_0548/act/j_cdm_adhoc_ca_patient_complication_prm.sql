
 select '$JOBNAME'  || ','  || cast(COUNT(*) as varchar(30)) ||',' as SOURCE_STRING 
 FROM (Select * from  EDWCDM.CA_PATIENT_COMPLICATION)a;