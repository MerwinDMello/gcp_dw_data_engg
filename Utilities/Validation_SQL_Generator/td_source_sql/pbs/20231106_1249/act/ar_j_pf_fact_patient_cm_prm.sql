select 'PBMAR250' ||','|| trim(zeroifnull(count(*))) ||',' as Source_String
 from edwpf.fact_rcom_ar_patient_lvl_cm