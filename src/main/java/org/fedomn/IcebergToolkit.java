package org.fedomn;


import org.fedomn.config.ConfigLoader;
import org.fedomn.config.HiveCatalogConfig;
import org.fedomn.iceberg.IcebergConnector;
import org.fedomn.security.KerberosAuthenticator;

public class IcebergToolkit {
  public static void main(String[] args) {
    HiveCatalogConfig hiveCatalogConfig = ConfigLoader.loadHiveCatalogConfigFromYaml();
    hiveCatalogConfig.initHiveConfig();

    System.out.println(hiveCatalogConfig);
    KerberosAuthenticator kerberosAuthenticator = new KerberosAuthenticator(hiveCatalogConfig);
    kerberosAuthenticator.authenticate();

    IcebergConnector iceberg = new IcebergConnector(hiveCatalogConfig);

    String functionType = args[0];
    if ("checkFileInManagement".equals(functionType)) {
      String filename = args[1];
      String dbName = args[2];
      String tableName = args[3];

      System.out.println("listTables: " + iceberg.listTables(dbName));

      System.out.println(
          "checkFileInManagement: " + filename + " in " + dbName + "." + tableName + " ...");
      iceberg.checkFileInManagement(filename, dbName, tableName);
    } else if ("showSnapshotFiles".equals(functionType)) {
      String snapshotId = args[1];
      String dbName = args[2];
      String tableName = args[3];

      System.out.println(
          "shouSnapshotFiles: " + snapshotId + " " + dbName + "." + tableName + " ...");
      iceberg.showSnapshotFiles(dbName, tableName, Long.parseLong(snapshotId));
    }
  }
}
