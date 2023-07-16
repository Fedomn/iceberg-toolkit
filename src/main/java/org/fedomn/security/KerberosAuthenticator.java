package org.fedomn.security;


import java.io.IOException;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.security.UserGroupInformation;
import org.fedomn.config.HiveCatalogConfig;
import org.fedomn.env.SystemEnv;

public class KerberosAuthenticator {
  private final HiveCatalogConfig hiveCatalogConfig;

  public KerberosAuthenticator(HiveCatalogConfig hiveCatalogConfig) {
    this.hiveCatalogConfig = hiveCatalogConfig;
  }

  public void authenticate() {
    System.setProperty("sun.security.krb5.debug", SystemEnv.getKrb5Debug());
    System.setProperty(
        "javax.security.auth.useSubjectCredsOnly", SystemEnv.getUseSubjectCredsOnly());
    System.setProperty("java.security.krb5.conf", hiveCatalogConfig.getKrb5Conf());

    HiveConf conf = hiveCatalogConfig.getHiveConf();
    UserGroupInformation.reset();
    System.out.printf(
        "hadoop.security.authentication: %s, isInitialized: %b, isSecurityEnabled: %b\n",
        conf.get("hadoop.security.authentication"),
        UserGroupInformation.isInitialized(),
        UserGroupInformation.isSecurityEnabled());

    UserGroupInformation.setConfiguration(conf);
    try {
      System.out.printf(
          "LoginUser: %s, ClientInfo: %s, %s\n",
          UserGroupInformation.getLoginUser(),
          hiveCatalogConfig.getClientPrincipal(),
          hiveCatalogConfig.getClientKeytab());
      System.out.printf(
          "after setConfiguration, isInitialized: %b, isSecurityEnabled: %b\n",
          UserGroupInformation.isInitialized(), UserGroupInformation.isSecurityEnabled());

      UserGroupInformation.loginUserFromKeytab(
          hiveCatalogConfig.getClientPrincipal(), hiveCatalogConfig.getClientKeytab());

      System.out.println(
          "Kerberos authentication succeeded with loginUser: "
              + UserGroupInformation.getLoginUser());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
