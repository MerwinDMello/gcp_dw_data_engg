```mermaid
---
config:
  flowchart:
    defaultRenderer: "elk"
title: Claims Batch Data Pipelines
---
%%{init: {'theme': 'base', 'themeVariables': { 'fontSize': '12pt' }}}%%
 graph LR
	classDef default fill:#90ee90;
	classDef start_dag fill:#add8e6;
	classDef end_dag fill:#00b38a;
	classDef bigquery fill:#4285f4;
	classDef sqlserver fill:#f2ac42;
	classDef default,start_dag,end_dag,bigquery,sqlserver stroke:#333,stroke-width:2px,font-size:14pt;

    Claims_Batching_Writeback(dag_outbound_claims_sqlserver_adhoc_claims_batch)
    Fact_Claims_Core[(Fact Claims)]
    Post_Poll(dag_postprocesspolling_claims_sqlserver_adhoc_claims_batch)
    Batching_Writeback[(Claims Batching)]

    Fact_Claims_Core --o Claims_Batching_Writeback --x Batching_Writeback
    Claims_Batching_Writeback == triggers ==> Post_Poll
	
	class Claims_Batching_Writeback start_dag
	class Post_Poll end_dag
	class Fact_Claims_Core bigquery
	class Batching_Writeback sqlserver
```