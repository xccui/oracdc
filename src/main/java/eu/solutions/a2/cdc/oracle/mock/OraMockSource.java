package eu.solutions.a2.cdc.oracle.mock;

import eu.solutions.a2.cdc.oracle.connection.OraPoolConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.sql.*;
import java.util.Properties;
import java.util.Random;

public class OraMockSource implements Runnable {

  private static Logger LOGGER = LoggerFactory.getLogger(OraMockSource.class);

  private static final String CONFIG_PATH = "etc/mock_source.properties";
  private static final String ORACLE_URL_KEY = "url";
  private static final String ORACLE_USER_KEY = "user";
  private static final String ORACLE_PASSWORD_KEY = "password";
  private static final String SLEEP_KEY = "sleep";

  private static final String INSERT_SQL =
      "INSERT INTO BOR_USER.TEST " +
          "(first_name, last_name, birth_date, height, sex, update_time) " +
          "VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)";

  private final String url;
  private final String user;
  private final String password;
  private final long sleep;

  private volatile boolean running = true;

  public OraMockSource() throws IOException {
    Properties properties = new Properties();
    File file = new File(CONFIG_PATH);
    LOGGER.info("Config path {}", file.getAbsolutePath());
    properties.load(new FileInputStream(file));
    url = properties.getProperty(ORACLE_URL_KEY);
    user = properties.getProperty(ORACLE_USER_KEY);
    password = properties.getProperty(ORACLE_PASSWORD_KEY);
    sleep = Long.parseLong(properties.getProperty(SLEEP_KEY));
    LOGGER.info("Configs: {}", properties.toString());
  }

  @Override
  public void run() {
    Random random = new Random();
    try {
      OraPoolConnectionFactory.init(url, user, password);
    } catch (SQLException ex) {
      ex.printStackTrace();
      running = false;
    }
    try (Connection connection = OraPoolConnectionFactory.getConnection()) {
      PreparedStatement statement = connection.prepareStatement(INSERT_SQL);
      int i = 0;
      while (running) {
        statement.setString(1, "foo");
        statement.setString(2, "bar");
        statement.setDate(3, new Date(System.currentTimeMillis() - 1000 * 60 * 60 * 24));
        if (i % 2 == 0) {
          statement.setFloat(4, 100 + 100 * random.nextFloat());
        } else {
          statement.setNull(4, JDBCType.FLOAT.getVendorTypeNumber());
        }
        statement.setString(5, "UNKNOWN");
        Thread.sleep(sleep);
        if (i % 3 == 0) {
          LOGGER.debug("Commit with i = {}", i);
          statement.executeBatch();
          connection.commit();
        } else {
          LOGGER.debug("Insert new with i = {}", i);
          statement.addBatch();
          statement.clearParameters();
        }
        ++i;
      }
    } catch (SQLException | InterruptedException throwables) {
      throwables.printStackTrace();
    }
  }

  public static void main(String[] args) throws IOException {
    new Thread(new OraMockSource()).start();
  }

}
