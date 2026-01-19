export ODBC_EXP_DB='METRIQ'

export JOBNAME='J_CR_PATIENT_RADIATION_ONCOLOGY'

export AC_EXP_SQL_STATEMENT="Select 'J_CR_PATIENT_RADIATION_ONCOLOGY'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING from EDWCR_Staging.CR_Patient_Radiation_Oncology_WRK"
export AC_ACT_SQL_STATEMENT="Select 'J_CR_PATIENT_RADIATION_ONCOLOGY'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING from EDWCR.CR_Patient_Radiation_Oncology;"
