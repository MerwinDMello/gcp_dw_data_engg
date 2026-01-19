declare dup_count int64;
declare ts datetime;
set ts = datetime_trunc(current_datetime('US/Central'), second);

begin
create temp table padict_temp as(
 SELECT DISTINCT Fld_Nbr,Topic,Item_Name,Data_Type,'L' AS Source_System_Cd  from {{ params.param_hr_stage_dataset_name }}.padict
 -- union
-- select DISTINCT FLD_NBR,Topic,Item_Name,Data_Type,'A' AS Source_System_Cd from {{ params.param_hr_stage_dataset_name }}.MSH_PADICT_STG
);

begin transaction;
merge {{ params.param_hr_core_dataset_name }}.ref_lawson_element tgt
using padict_temp src
on tgt.lawson_element_num = src.fld_nbr
and tgt.source_system_code = src.source_system_cd
when matched then
update set 
tgt.lawson_topic_code = src.topic,
tgt.lawson_element_desc = src.item_name,
tgt.lawson_element_type_flag = src.data_type,
tgt.dw_last_update_date_time = ts
when not matched then
insert(lawson_element_num,lawson_topic_code, lawson_element_desc, lawson_element_type_flag, source_system_code, dw_last_update_date_time)
values(fld_nbr, topic, item_name, data_type, source_system_cd, ts);



insert into {{ params.param_hr_core_dataset_name }}.ref_lawson_element(lawson_element_num, source_system_code)
SELECT CAST(field_key as  int64) as Field_Key,'L' as Source_System_Code  from {{ params.param_hr_stage_dataset_name }}.hrempusf 
WHERE CAST(field_key as  int64) NOT IN (SELECT DISTINCT Lawson_Element_Num FROM {{ params.param_hr_core_dataset_name }}.ref_lawson_element);
-- union 
-- SELECT CAST(field_key as  SMALLINT) as Field_Key,'A' as Source_System_Code  from {{ params.param_hr_stage_dataset_name }}.MSH_HRempusf_STG 
-- WHERE (CAST(field_key as  SMALLINT),Source_System_Code )  NOT IN (SELECT DISTINCT Lawson_Element_Num,Source_System_Code 
--  FROM {{ params.param_hr_core_dataset_name }}.Ref_Lawson_Element);


SET
  DUP_COUNT = (
  SELECT
    COUNT(*)
  FROM (
    SELECT
       lawson_element_num, source_system_code
    FROM
      {{ params.param_hr_core_dataset_name }}.ref_lawson_element
    GROUP BY
       lawson_element_num, source_system_code
    HAVING
      COUNT(*) > 1 ) );
IF
  DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION; RAISE
USING
  MESSAGE = CONCAT('Duplicates are not allowed in the table: ref_lawson_element');
  ELSE
COMMIT TRANSACTION;
END IF;
END;


