get-childitem *.sql 
#| foreach { rename-item $_ $_.Name.Replace("_prm.sql", ".sql") }