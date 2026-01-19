sudo su

cd /opt/dwh-migration-tools-v1.0.27/bin

./dwh-migration-dumper --connector teradata --url "jdbc:teradata://SERVER_ADDRESS/SSLMODE=DISABLE" --user SERVICE_ACCOUNT --password PASSWORD --driver TERADATA_JDBC_JAR_FULL_PATH --assessment