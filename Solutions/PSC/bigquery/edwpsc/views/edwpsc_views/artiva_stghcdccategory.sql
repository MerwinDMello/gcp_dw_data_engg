CREATE OR REPLACE VIEW edwpsc_views.`artiva_stghcdccategory`
AS SELECT
  `artiva_stghcdccategory`.hcdcdesc,
  `artiva_stghcdccategory`.hcdcid
  FROM
    edwpsc_base_views.`artiva_stghcdccategory`
;