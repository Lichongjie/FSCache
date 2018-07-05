package alluxio.client;

import alluxio.util.io.PathUtils;
import com.google.common.base.Preconditions;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.util.Properties;

/**
 * Proxy configuration.
 */
public enum Configuration {
  INSTANCE;

  /** The default property file name. */
  private static final String DEFAULT_PROPERTY_FILE = "gateway-default.properties";
  /** The user-specific property file name. */
  private static final String PROPERTY_FILE = "gateway.properties";
  /** The singleton maintains all the properties. */
  private static final Properties PROPERTIES = new Properties();

  private static final long KB = 1024L;
  private static final long MB = 1024L * KB;
  private static final long GB = 1024L * MB;
  private static final long TB = 1024L * GB;
  private static final long PB = 1024L * TB;

  static {
    // Default properties
    InputStream is =
        Configuration.class.getClassLoader().getResourceAsStream(DEFAULT_PROPERTY_FILE);
    Preconditions.checkNotNull(is);
    try {
      PROPERTIES.load(is);
    } catch (IOException e) {
      throw new Error("Failed to load the property file", e);
    }

    // Properties in proxy.properties
    String confDir = System.getProperty(Constants.CONF_DIR);
    if (confDir != null) {
      String userPropertyFileName = PathUtils.concatPath(confDir, PROPERTY_FILE);
      Properties userProperties = loadUserProperties(userPropertyFileName);
      if (userProperties != null) {
        PROPERTIES.putAll(userProperties);
      }
    }

    // System properties
    Properties systemProperties = System.getProperties();
    PROPERTIES.putAll(systemProperties);
  }

  /**
   * Loads the user specified property file.
   *
   * @param path the property file path
   * @return the user specified properties
   */
  private static Properties loadUserProperties(String path) {
    try {
      Properties ret = new Properties();
      FileInputStream in = new FileInputStream(path);
      ret.load(in);
      return ret;
    } catch (Exception e) {
      return null;
    }
  }

  /**
   * Gets the bytes of the value for the given key.
   *
   * @param attr the key to get the value for
   * @return the bytes of the value for the given key
   */
  public long getBytes(String attr) {
    String rawValue = PROPERTIES.getProperty(attr);
    Preconditions.checkNotNull(rawValue);
    return parseSpaceSize(rawValue);
  }

  /**
   * Gets the boolean representation of the value for the given key.
   *
   * @param attr the key to get the value for
   * @return the value for the given key as a {@code boolean}
   */
  public boolean getBoolean(String attr) {
    String rawValue = PROPERTIES.getProperty(attr);
    Preconditions.checkNotNull(rawValue);
    return Boolean.parseBoolean(rawValue);
  }

  /**
   * Gets a short property.
   *
   * @param attr the attribute
   * @return a short value
   */
  public short getShort(String attr) {
    String value = PROPERTIES.getProperty(attr);
    Preconditions.checkNotNull(value);
    return Short.parseShort(value);
  }

  /**
   * Gets an int property.
   *
   * @param attr the attribute
   * @return an int value
   */
  public int getInt(String attr) {
    String value = PROPERTIES.getProperty(attr);
    Preconditions.checkNotNull(value);
    return Integer.parseInt(value);
  }

  /**
   * Gets a long property.
   *
   * @param attr the attribute
   * @return a long value
   */
  public long getLong(String attr) {
    String value = PROPERTIES.getProperty(attr);
    Preconditions.checkNotNull(value);
    return Long.parseLong(value);
  }

  /**
   * Gets a float property.
   *
   * @param attr the attribute
   * @return a float value
   */
  public float getFloat(String attr) {
    String value = PROPERTIES.getProperty(attr);
    Preconditions.checkNotNull(value);
    return Float.parseFloat(value);
  }

  /**
   * Gets a double property.
   *
   * @param attr the attribute
   * @return a double value
   */
  public double getDouble(String attr) {
    String value = PROPERTIES.getProperty(attr);
    Preconditions.checkNotNull(value);
    return Double.parseDouble(value);
  }

  /**
   * Gets a string property.
   *
   * @param attr the attribute
   * @return a string value
   */
  public String getString(String attr) {
    String value = PROPERTIES.getProperty(attr);
    Preconditions.checkNotNull(value);
    return value;
  }

  /**
   * Parses a String size to Bytes.
   *
   * @param spaceSize the size of a space, e.g. 10GB, 5TB, 1024
   * @return the space size in bytes
   */
  private static long parseSpaceSize(String spaceSize) {
    double alpha = 0.0001;
    String ori = spaceSize;
    String end = "";
    int index = spaceSize.length() - 1;
    while (index >= 0) {
      if (spaceSize.charAt(index) > '9' || spaceSize.charAt(index) < '0') {
        end = spaceSize.charAt(index) + end;
      } else {
        break;
      }
      index--;
    }
    spaceSize = spaceSize.substring(0, index + 1);
    double ret = Double.parseDouble(spaceSize);
    end = end.toLowerCase();
    if (end.isEmpty() || end.equals("b")) {
      return (long) (ret + alpha);
    } else if (end.equals("kb")) {
      return (long) (ret * KB + alpha);
    } else if (end.equals("mb")) {
      return (long) (ret * MB + alpha);
    } else if (end.equals("gb")) {
      return (long) (ret * GB + alpha);
    } else if (end.equals("tb")) {
      return (long) (ret * TB + alpha);
    } else if (end.equals("pb")) {
      BigDecimal pBDecimal = new BigDecimal(PB);
      return pBDecimal.multiply(BigDecimal.valueOf(ret)).longValue();
    } else {
      throw new IllegalArgumentException("Fail to parse " + ori + " to bytes");
    }
  }
}
