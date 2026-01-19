BEGIN

CREATE temp TABLE t1 AS
SELECT * FROM {{ params.param_hr_stage_dataset_name }}.ref_address_type;

TRUNCATE TABLE {{ params.param_hr_stage_dataset_name }}.ref_address_type ;

INSERT INTO {{ params.param_hr_stage_dataset_name }}.ref_address_type
SELECT  
    addr_type_code 
    , addr_type_desc 
    , CASE
        WHEN TRIM(addr_type_code) IN ('EMP','LOC','PRS') THEN 'L'
        WHEN TRIM(addr_type_code) IN ('NCA','SHA') THEN 'C'
        ELSE
            'M'
      END AS source_system_code,
    , timestamp_trunc(current_datetime('US/Central'), SECOND)
FROM t1
WHERE LENGTH(NULLIF(TRIM(addr_type_code),'')) > 0 

END;
