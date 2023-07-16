package org.fedomn.config;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.io.File;

public class ConfigLoader {
  public static final String CONFIG_YML =
      String.format("%s/.iceberg_toolkit.yaml", System.getProperty("user.home"));

  public static HiveCatalogConfig loadHiveCatalogConfigFromYaml() {
    try {
      ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
      mapper.findAndRegisterModules();
      return mapper.readValue(new File(CONFIG_YML), HiveCatalogConfig.class);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
