package org.fedomn.env;

public final class SystemEnv {
  public static String getKrb5Debug() {
    return System.getProperty("sun.security.krb5.debug", "false");
  }

  public static String getUseSubjectCredsOnly() {
    return System.getProperty("javax.security.auth.useSubjectCredsOnly", "false");
  }
}
