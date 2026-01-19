CREATE OR REPLACE VIEW edwpsc_base_views.hin_secref_facility
AS SELECT
	company_code,
	user_id,
	co_id
  FROM
    edwpsc.hin_secref_facility
;
