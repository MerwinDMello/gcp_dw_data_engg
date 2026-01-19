BEGIN
DECLARE
  DUP_COUNT INT64;


TRUNCATE TABLE {{ params.param_hr_core_dataset_name }}.question_legacy_survey_xwalk;

BEGIN TRANSACTION;
INSERT INTO {{ params.param_hr_core_dataset_name }}.question_legacy_survey_xwalk(
    SELECT * FROM {{ params.param_hr_stage_dataset_name }}.question_legacy_survey_xwalk_stg
);


        SET DUP_COUNT = ( 
        select count(*)
        from (
        select
           question_id
        from {{ params.param_hr_core_dataset_name }}.question_legacy_survey_xwalk
        group by question_id
        having count(*) > 1
        )
    );
        IF
         DUP_COUNT <> 0 THEN
          ROLLBACK TRANSACTION; RAISE
            USING
            MESSAGE = CONCAT('Duplicates are not allowed in the table: {{ params.param_hr_core_dataset_name }}.question_legacy_survey_xwalk');
        ELSE
        COMMIT TRANSACTION;
        END IF
  ;
END;