# iceberg-toolkit

iceberg toolkit to inspect table internal, currently only support kerberos enabled hive metastore.

build from source: `mvn clean package -DskipTests`

## security

To use Kerberos enabled HMS you must prepare the config file (~/.iceberg_toolkit.yaml):
```yaml
metastoreUri: thrift://localhost:9083
metastoreWarehouseDir: /user/hive/warehouse
servicePrincipal: hive/_HOST@DEV.COM
clientPrincipal: <client-principal>
clientKeytab: <client-keytab>
krb5Conf: /etc/krb5.conf
configResources:
  - /etc/hive/conf/hive-site.xml
  - /etc/hadoop/conf/core-site.xml
  - /etc/hadoop/conf/hdfs-site.xml
```

## usage

```shell
java -jar iceberg-toolkit-1.0.0.jar checkFileInManagement file.parquet db table
java -jar iceberg-toolkit-1.0.0.jar showSnapshotFiles snapshotId db table
```


## others

use `mvn dependency:tree -Dverbose -Dincludes=org.slf4j:slf4j-\*` to check the dependency tree.