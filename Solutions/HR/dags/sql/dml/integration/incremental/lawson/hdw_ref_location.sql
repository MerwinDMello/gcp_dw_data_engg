declare dup_count int64;
declare ts datetime;
set ts = datetime_trunc(current_datetime('US/Central'), second);

BEGIN
CREATE TEMP TABLE ref_location_temp as (
SELECT X.Locat_Code Locat_Code,				
               
              pcds.Description Locat_Desc,
			   X.Src_system_Code
FROM 
(
SELECT COALESCE(Code,'') Locat_Code, 'L' as Src_system_Code
FROM {{ params.param_hr_stage_dataset_name }}.pcodes
WHERE Type = 'LO'
UNION DISTINCT
SELECT  distinct(COALESCE(Locat_Code,'')) Locat_Code, 'L' as Src_system_Code
FROM {{ params.param_hr_stage_dataset_name }}.paemppos
UNION DISTINCT
SELECT  distinct(COALESCE(Locat_Code,'')) Locat_Code, 'L' as Src_system_Code
FROM {{ params.param_hr_stage_dataset_name }}.pajobreq
UNION DISTINCT
SELECT  distinct(COALESCE(Locat_Code,'')) Locat_Code, 'L' as Src_system_Code
FROM {{ params.param_hr_stage_dataset_name }}.paemployee
UNION DISTINCT
SELECT  distinct(COALESCE(Locat_Code,'')) Locat_Code, 'L' as Src_system_Code
FROM {{ params.param_hr_stage_dataset_name }}.paposition
-- UNION
-- SELECT COALESCE(Code,'') Locat_Code, 'A' as Src_system_Code
-- FROM {{ params.param_hr_stage_dataset_name }}.MSH_Pcodes_Stg
-- WHERE R_Type = 'LO'
-- UNION
-- SELECT  COALESCE(Locat_Code,'') Locat_Code, 'A' as Src_system_Code
-- FROM {{ params.param_hr_stage_dataset_name }}.MSH_Paemppos_Stg
-- UNION
-- SELECT  COALESCE(Locat_Code,'') Locat_Code, 'A' as Src_system_Code
-- FROM {{ params.param_hr_stage_dataset_name }}.MSH_Pajobreq_Stg
-- UNION
-- SELECT  COALESCE(Locat_Code,'') Locat_Code, 'A' as Src_system_Code
-- FROM {{ params.param_hr_stage_dataset_name }}.MSH_Paemployee_Stg
-- UNION
-- SELECT  COALESCE(Locat_Code,'') Locat_Code, 'A' as Src_system_Code
-- FROM {{ params.param_hr_stage_dataset_name }}.MSH_Paposition_Stg
)X 

LEFT OUTER JOIN (select distinct(PCD.code), PCD.Description from {{ params.param_hr_stage_dataset_name }}.pcodes PCD
									--  union
									--  select MH_PCD.code, MH_PCD.Description from {{ params.param_hr_stage_dataset_name }}.MSH_Pcodes_Stg MH_PCD
                  ) pcds
on X.Locat_Code = pcds.Code 
where CHAR_LENGTH(TRIM(LOCAT_CODE)) > 0);


begin transaction;
merge {{ params.param_hr_core_dataset_name }}.ref_location tgt
using ref_location_temp src
on tgt.location_code = src.locat_code
when matched then 
update set
tgt.location_desc = src.locat_desc,
tgt.source_system_code = src.src_system_code ,
tgt.dw_last_update_date_time = ts
when not matched then
insert(location_code, location_desc, source_system_code, dw_last_update_date_time)
values(locat_code, locat_desc, src_system_code, ts)
;
SET
  DUP_COUNT = (
  SELECT
    COUNT(*)
  FROM (
    SELECT
       location_code
    FROM
      {{ params.param_hr_core_dataset_name }}.ref_location
    GROUP BY
       location_code
    HAVING
      COUNT(*) > 1 ) );
IF
  DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION; RAISE
USING
  MESSAGE = CONCAT('Duplicates are not allowed in the table: ref_location');
  ELSE
COMMIT TRANSACTION;
END IF;
END;
