CREATE OR REPLACE VIEW edwpsc_base_views.`artiva_stgpenoteshistory`
AS SELECT
  `artiva_stgpenoteshistory`.penoteshistorykey,
  `artiva_stgpenoteshistory`.pspeppikey,
  `artiva_stgpenoteshistory`.notecount,
  `artiva_stgpenoteshistory`.notedate,
  `artiva_stgpenoteshistory`.notetime,
  `artiva_stgpenoteshistory`.notedatetime,
  `artiva_stgpenoteshistory`.notetype,
  `artiva_stgpenoteshistory`.notecreatedbyuserid,
  `artiva_stgpenoteshistory`.note,
  `artiva_stgpenoteshistory`.notesource,
  `artiva_stgpenoteshistory`.sourceaprimarykeyvalue,
  `artiva_stgpenoteshistory`.insertedby,
  `artiva_stgpenoteshistory`.inserteddtm,
  `artiva_stgpenoteshistory`.modifiedby,
  `artiva_stgpenoteshistory`.modifieddtm,
  `artiva_stgpenoteshistory`.dwlastupdatedatetime
  FROM
    edwpsc.`artiva_stgpenoteshistory`
;