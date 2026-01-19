```mermaid
---
config:
  flowchart:
    defaultRenderer: "elk"
title: EP Payer Reconciliation Data Pipelines
---
%%{init: {'theme': 'base', 'themeVariables': { 'fontSize': '12pt' }}}%%
 graph TD
	classDef default fill:#90ee90;
	classDef start_dag fill:#add8e6;
	classDef end_dag fill:#00b38a;
	classDef bigquery fill:#4285f4;
	classDef sqlserver fill:#f2ac42;
	classDef default,start_dag,end_dag,bigquery,sqlserver stroke:#333,stroke-width:2px,font-size:14pt;

    Pre_Poll(dag_preprocesspolling_remits_sqlserver_daily_ep_payer_recon **6:15 PM Mon-Fri**)
    Ingest(dag_ingest_remits_sqlserver_daily_ep_payer_recon)
    Integrate(dag_integrate_remits_sqlserver_daily_ep_payer_recon)
    Core_Table_DW_Counts_Writeback(dag_outbound_remits_sqlserver_daily_dwcount_ep_payer_recon)
    Post_Poll(dag_postprocesspolling_remits_sqlserver_daily_ep_payer_recon)
    EP_Remit_Recon_Source[(EP Remit Reconciliation Source)]
	EP_Payer_Recon_Staging[(EP Payer Recon Staging)]
	EP_Payer_Recon_Core[(EP Payer Recon Core)]
    DW_Counts_Writeback[(Core Table DW Counts)]

    Pre_Poll == triggers ==> Ingest == triggers ===> Integrate == triggers ===> Core_Table_DW_Counts_Writeback == triggers ==> Post_Poll
	EP_Remit_Recon_Source --o Ingest --x EP_Payer_Recon_Staging --o Integrate --x EP_Payer_Recon_Core --o Core_Table_DW_Counts_Writeback --x DW_Counts_Writeback
	
	class Pre_Poll start_dag
	class Post_Poll end_dag
	class EP_Payer_Recon_Staging,EP_Payer_Recon_Core bigquery
	class EP_Remit_Recon_Source,DW_Counts_Writeback sqlserver
```