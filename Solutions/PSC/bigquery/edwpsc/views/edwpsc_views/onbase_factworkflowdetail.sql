CREATE OR REPLACE VIEW edwpsc_views.`onbase_factworkflowdetail`
AS SELECT
  `onbase_factworkflowdetail`.documenthandle,
  `onbase_factworkflowdetail`.keyworddocumenthandle,
  `onbase_factworkflowdetail`.keywordentrydate,
  `onbase_factworkflowdetail`.keywordexitdate,
  `onbase_factworkflowdetail`.keywordlifecyclename,
  `onbase_factworkflowdetail`.keyworddocumenttype,
  `onbase_factworkflowdetail`.keywordqueuename,
  `onbase_factworkflowdetail`.keyworduserid,
  `onbase_factworkflowdetail`.insertedby,
  `onbase_factworkflowdetail`.inserteddtm,
  `onbase_factworkflowdetail`.modifiedby,
  `onbase_factworkflowdetail`.modifieddtm,
  `onbase_factworkflowdetail`.onbaseworkflowdetailkey,
  `onbase_factworkflowdetail`.dwlastupdatedatetime
  FROM
    edwpsc_base_views.`onbase_factworkflowdetail`
;