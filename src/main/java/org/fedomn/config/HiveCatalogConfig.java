package org.fedomn.config;

import static org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars.CLIENT_KERBEROS_PRINCIPAL;
import static org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars.KERBEROS_PRINCIPAL;
import static org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars.THRIFT_URIS;
import static org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars.USE_THRIFT_SASL;
import static org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars.WAREHOUSE;

import com.fasterxml.jackson.annotation.JsonIgnore;
import java.util.List;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;

public class HiveCatalogConfig {
  private String metastoreUri;
  private String metastoreWarehouseDir;
  private String servicePrincipal;
  private String clientPrincipal;
  private String clientKeytab;
  private String krb5Conf;
  private List<String> configResources;

  private HiveConf hiveConf;

  public HiveCatalogConfig() {}

  public HiveCatalogConfig(
      String metastoreUri,
      String metastoreWarehouseDir,
      String servicePrincipal,
      String clientPrincipal,
      String clientKeytab,
      String krb5Conf,
      List<String> configResources) {
    this.metastoreUri = metastoreUri;
    this.metastoreWarehouseDir = metastoreWarehouseDir;
    this.servicePrincipal = servicePrincipal;
    this.clientPrincipal = clientPrincipal;
    this.clientKeytab = clientKeytab;
    this.krb5Conf = krb5Conf;
    this.configResources = configResources;
  }

  public void initHiveConfig() {
    HiveConf conf = new HiveConf();
    conf.set("hadoop.security.authentication", "kerberos");
    for (String resource : getConfigResources()) {
      conf.addResource(new Path(resource));
    }
    MetastoreConf.setVar(conf, THRIFT_URIS, getMetastoreUri());
    MetastoreConf.setVar(conf, WAREHOUSE, getMetastoreWarehouseDir());
    MetastoreConf.setVar(conf, KERBEROS_PRINCIPAL, getServicePrincipal());
    MetastoreConf.setVar(conf, CLIENT_KERBEROS_PRINCIPAL, getClientPrincipal());
    MetastoreConf.setBoolVar(conf, USE_THRIFT_SASL, true);
    this.hiveConf = conf;
  }

  public String getMetastoreUri() {
    return metastoreUri;
  }

  public void setMetastoreUri(String metastoreUri) {
    this.metastoreUri = metastoreUri;
  }

  public String getMetastoreWarehouseDir() {
    return metastoreWarehouseDir;
  }

  public void setMetastoreWarehouseDir(String metastoreWarehouseDir) {
    this.metastoreWarehouseDir = metastoreWarehouseDir;
  }

  public String getServicePrincipal() {
    return servicePrincipal;
  }

  public void setServicePrincipal(String servicePrincipal) {
    this.servicePrincipal = servicePrincipal;
  }

  public String getClientPrincipal() {
    return clientPrincipal;
  }

  public void setClientPrincipal(String clientPrincipal) {
    this.clientPrincipal = clientPrincipal;
  }

  public String getClientKeytab() {
    return clientKeytab;
  }

  public void setClientKeytab(String clientKeytab) {
    this.clientKeytab = clientKeytab;
  }

  public String getKrb5Conf() {
    return krb5Conf;
  }

  public void setKrb5Conf(String krb5Conf) {
    this.krb5Conf = krb5Conf;
  }

  public List<String> getConfigResources() {
    return configResources;
  }

  public void setConfigResources(List<String> configResources) {
    this.configResources = configResources;
  }

  @JsonIgnore
  public HiveConf getHiveConf() {
    return hiveConf;
  }

  @JsonIgnore
  public void setHiveConf(HiveConf hiveConf) {
    this.hiveConf = hiveConf;
  }

  @Override
  public String toString() {
    return "HiveCatalogConfig{"
        + "metastoreUri='"
        + metastoreUri
        + '\''
        + ", metastoreWarehouseDir='"
        + metastoreWarehouseDir
        + '\''
        + ", servicePrincipal='"
        + servicePrincipal
        + '\''
        + ", clientPrincipal='"
        + clientPrincipal
        + '\''
        + ", clientKeytab='"
        + clientKeytab
        + '\''
        + ", krb5Conf='"
        + krb5Conf
        + '\''
        + ", configResources="
        + configResources
        + '}';
  }
}
