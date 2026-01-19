export JOBNAME='J_REF_IMAGING_TYPE'
export AC_EXP_SQL_STATEMENT="select 'J_REF_IMAGING_TYPE'+ ',' + 
cast(count(*) as varchar(20)) + ',' AS SOURCE_STRING from [Navadhoc].[dbo].[RefImagingType]" 

export AC_ACT_SQL_STATEMENT="select 'J_REF_IMAGING_TYPE' || ',' || CAST(COUNT(*) AS VARCHAR(20)) || ',' AS SOURCE_STRING from EDWCR.Ref_Imaging_Type"
