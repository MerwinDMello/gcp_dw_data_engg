CREATE OR REPLACE VIEW edwpsc_views.`ecw_reportsecuritypbiuseremployeetype`
AS SELECT
  `ecw_reportsecuritypbiuseremployeetype`.useremployeetype,
  `ecw_reportsecuritypbiuseremployeetype`.employeetype
  FROM
    edwpsc_base_views.`ecw_reportsecuritypbiuseremployeetype`
;