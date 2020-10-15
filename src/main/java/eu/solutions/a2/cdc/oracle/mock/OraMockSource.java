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

  private static final Logger LOGGER = LoggerFactory.getLogger(OraMockSource.class);

  private static final String CONFIG_PATH = "etc/mock_source.properties";
  private static final String ORACLE_URL_KEY = "url";
  private static final String ORACLE_USER_KEY = "user";
  private static final String ORACLE_PASSWORD_KEY = "password";
  private static final String SLEEP_KEY = "sleep";

  private static final String USER_GROUP_TEST_INSERT_SQL =
      "INSERT INTO BOR_CDC.USER_GROUP_TEST " +
          "(GROUP_ID, GROUP_NAME, GROUP_STATUS) " +
          "VALUES (?, ?, ?)";

  private static final String USER_TEST_INSERT_SQL =
      "INSERT INTO BOR_CDC.USER_TEST " +
          "(FIRST_NAME, LAST_NAME, BIRTH_DATE, HEIGHT, SEX, UPDATE_TIME, GROUP_ID) " +
          "VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP, ?)";

  private static final String USER_GROUP_TEST_UPDATE_SQL =
      "UPDATE BOR_CDC.USER_GROUP_TEST" +
          " SET GROUP_STATUS = 'P'" +
          " WHERE GROUP_ID = ?";

  private static final String USER_TEST_QUERY_LAST_SQL =
      "SELECT USER_ID FROM BOR_CDC.USER_TEST WHERE GROUP_ID = ?";

  private static final String USER_TEST_DELETE_SQL =
      "DELETE FROM BOR_CDC.USER_TEST WHERE USER_ID = ?";

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
      int i = 0;
      PreparedStatement userGroupInsertStat = connection.prepareStatement(USER_GROUP_TEST_INSERT_SQL);
      PreparedStatement userInsertStat = connection.prepareStatement(USER_TEST_INSERT_SQL);
      PreparedStatement userGroupUpdateStat = connection.prepareStatement(USER_GROUP_TEST_UPDATE_SQL);
      PreparedStatement userQueryLastStat = connection.prepareStatement(USER_TEST_QUERY_LAST_SQL);
      PreparedStatement userDeleteStat = connection.prepareStatement(USER_TEST_DELETE_SQL);
      String groupId;
      int randomInt;
      while (running) {
        // 1. Insert the group and a bunch of users
        randomInt = random.nextInt(1000);
        groupId = System.currentTimeMillis() + "_" + randomInt;
        userGroupInsertStat.setString(1, groupId);
        userGroupInsertStat.setString(2, String.valueOf(randomInt));
        userGroupInsertStat.setString(3, "U");
        userGroupInsertStat.execute();

        int users = random.nextInt(10) + 1;
        for (int j = 0; j < users; ++j) {
          userInsertStat.setString(1, "FirstName_" + randomInt);
          userInsertStat.setString(2, "LastName_" + randomInt);
          userInsertStat.setDate(3, new Date(System.currentTimeMillis() - 1000 * 60 * 60 * 24));
          if (i % 2 == 0) {
            userInsertStat.setFloat(4, 100 + 100 * random.nextFloat());
          } else {
            userInsertStat.setNull(4, JDBCType.FLOAT.getVendorTypeNumber());
          }
          userInsertStat.setString(5, "UNKNOWN");
          userInsertStat.setString(6, groupId);
          userInsertStat.addBatch();
          userInsertStat.clearParameters();
          ++i;
        }
        userInsertStat.executeBatch();
        connection.commit();
        LOGGER.debug("Insert " + users + " users with groupId = "+ groupId);
        Thread.sleep(sleep);

        // 2. Update the status of the group
        userGroupUpdateStat.setString(1, groupId);
        userGroupUpdateStat.execute();
        connection.commit();
        LOGGER.debug("Update the group with groupId = " + groupId);
        Thread.sleep(sleep);

        // 3. Delete a user from the last group
        userQueryLastStat.setString(1, groupId);
        ResultSet rs = userQueryLastStat.executeQuery();
        if (rs.next()) {
          long userId = rs.getLong(1);
          userDeleteStat.setLong(1, userId);
          userDeleteStat.execute();
          rs.close();
          connection.commit();
          LOGGER.debug("Remove a user with user_id = " + userId);
        }
        Thread.sleep(sleep);
      }
    } catch (SQLException | InterruptedException throwables) {
      throwables.printStackTrace();
    }
  }

  public static void main(String[] args) throws IOException {
    new Thread(new OraMockSource()).start();
  }

}
