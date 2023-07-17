package org.fedomn.iceberg;


import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.iceberg.io.CloseableIterable;
import org.fedomn.config.HiveCatalogConfig;

public class IcebergConnector {

  private final HiveCatalog hiveCatalog;

  public IcebergConnector(HiveCatalogConfig config) {
    this.hiveCatalog = initHiveCatalog(config);
  }

  private HiveCatalog initHiveCatalog(HiveCatalogConfig config) {
    HiveCatalog hiveCatalog = new HiveCatalog();
    hiveCatalog.setConf(config.getHiveConf());

    Map<String, String> properties = new HashMap<>();
    properties.put("uri", config.getMetastoreUri());
    properties.put("warehouse", config.getMetastoreWarehouseDir());
    properties.put("list-all-tables", "true");
    hiveCatalog.initialize("hive", properties);
    return hiveCatalog;
  }

  public Table loadTable(String dbName, String tableName) {
    TableIdentifier identifier = TableIdentifier.of(dbName, tableName);
    return hiveCatalog.loadTable(identifier);
  }

  public void checkFileInManagement(String filename, String dbName, String tableName) {
    Table table = hiveCatalog.loadTable(TableIdentifier.of(dbName, tableName));
    Iterable<Snapshot> snapshots = table.snapshots();
    long currentSnapshotId = table.currentSnapshot().snapshotId();
    for (Snapshot snapshot : snapshots) {
      if (snapshot.snapshotId() == currentSnapshotId) {
        System.out.println("CurrentSnapshotInfo: " + snapshot);
      } else {
        System.out.println("SnapshotInfo: " + snapshot);
      }
    }

    for (Snapshot snapshot : snapshots) {
      TableScan scan = table.newScan().useSnapshot(snapshot.snapshotId());
      CloseableIterable<FileScanTask> scanTasks = scan.planFiles();
      for (FileScanTask scanTask : scanTasks) {
        if (scanTask.file().path().toString().contains(filename)) {
          System.out.println(
              "File "
                  + filename
                  + " is data file in snapshot: "
                  + snapshot.snapshotId()
                  + ", full path is "
                  + scanTask.file().path().toString());
          return;
        }
        for (DeleteFile deleteFile : scanTask.deletes()) {
          if (deleteFile.path().toString().contains(filename)) {
            System.out.println(
                "File "
                    + filename
                    + " is delete file in snapshot: "
                    + snapshot.snapshotId()
                    + ", full path is "
                    + deleteFile.path().toString());
            return;
          }
        }
      }
    }
    System.out.println("File " + filename + " is not in management.");
  }

  public List<String> listTables(String dbName) {
    List<TableIdentifier> tables = hiveCatalog.listTables(Namespace.of(dbName));
    return tables.stream().map(TableIdentifier::name).collect(Collectors.toList());
  }
}
