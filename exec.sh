java -cp DatabricksJDBC42.jar:./class --add-opens java.base/java.nio=ALL-UNNAMED  com.databricks.norfolk.RKMQuery e2-demo-field-eng.cloud.databricks.com 443 /sql/1.0/warehouses/ead10bf07050390f hive_metastore token <replacemewithtoken> 'select * from samples.nyctaxi.trips limit 10'
