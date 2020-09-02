package eu.solutions.a2.cdc.oracle;

import eu.solutions.a2.cdc.oracle.connection.OraDictSqlTexts;
import eu.solutions.a2.cdc.oracle.connection.OraPoolConnectionFactory;
import eu.solutions.a2.cdc.oracle.jmx.OraCdcLogMinerMgmt;
import eu.solutions.a2.cdc.oracle.utils.OraSqlUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;

public class LocalMain {
  private static final Logger LOGGER = LoggerFactory.getLogger(LocalMain.class);

  private static final int SCHEMA_TYPE = ParamConstants.SCHEMA_TYPE_INT_DEBEZIUM; // use debezium schema
  private static final String TEMP_DIR = System.getProperty("java.io.tmpdir") + "oracdc";
//  private static final String STATE_FILE_NAME = TEMP_DIR + "/" + "oracdc.state";
  private static final String TOPIC = "cdc_test";
  private static final BlockingQueue<OraCdcTransaction> transactionsQueue = new LinkedBlockingDeque<>();
  private static final int TOPIC_NAME_STYLE_TABLE = ParamConstants.TOPIC_NAME_STYLE_INT_TABLE;
  private static final String TOPIC_NAME_DELIMITER = ParamConstants.TOPIC_NAME_DELIMITER_UNDERSCORE;

  private static final List<String> INCLUDE_LIST = Arrays.asList(
      "FND_CONCURRENT_QUEUES",
      "FND_CONCURRENT_REQUESTS",
      "FND_CONC_PROG_ONSITE_INFO",
      "FND_OAM_APP_SYS_STATUS",
      "FND_LOGINS",
      "FND_CONC_PP_ACTIONS",
      "FND_CONC_RELEASE_CLASSES",
      "FND_CONC_PROG_ONSITE_INFO",
      "AQ$_WF_CONTROL_L",
      "WF_CONTROL");

  public static void main(String[] args) throws Exception {
    LOGGER.info("Set temporary directory to {}", TEMP_DIR);
    String url = System.getProperty("ORACDC_URL");
    String user = System.getProperty("ORACDC_USER");
    String password = System.getProperty("ORACDC_PASSWORD");
    OraPoolConnectionFactory.init(url, user, password);
    try (Connection connection = OraPoolConnectionFactory.getConnection()) {
      OraRdbmsInfo rdbmsInfo = new OraRdbmsInfo(connection);
      LOGGER.info("Connected to $ORACLE_SID={}, version={}, running on {}, OS {}.",
          rdbmsInfo.getInstanceName(), rdbmsInfo.getVersionString(), rdbmsInfo.getHostName(), rdbmsInfo.getPlatformName());
    }

    try (Connection connDictionary = OraPoolConnectionFactory.getConnection()) {
      OraRdbmsInfo rdbmsInfo = OraRdbmsInfo.getInstance();
      final String tableList = OraSqlUtils.parseTableSchemaList(
          false, OraSqlUtils.MODE_WHERE_ALL_OBJECTS, INCLUDE_LIST);
      final String objectList = rdbmsInfo.getMineObjectsIds(connDictionary, false, tableList);

      String mineDataSql = generateMineDataSql(objectList);
      LOGGER.info("Mine data sql: {}", mineDataSql);
      String checkTableSql = generateCheckTableSql(tableList);
      LOGGER.info("Check table sql: {}", checkTableSql);
      String initalLoadSql = generateInitialLoadSql(tableList);
      LOGGER.info("Generate initial load sql: {}", initalLoadSql);
      Map<String, String> partition = getPartition(rdbmsInfo);
      OraDumpDecoder odd = new OraDumpDecoder(rdbmsInfo.getDbCharset(), rdbmsInfo.getDbNCharCharset());
      Map<Long, OraTable4LogMiner> tablesInProcessing = buildInitialLoadTableList(initalLoadSql, rdbmsInfo, odd, partition);
      Path queuesRoot = FileSystems.getDefault().getPath(TEMP_DIR);
      OraCdcLogMinerMgmt metrics = new OraCdcLogMinerMgmt(rdbmsInfo, "name", null);

      OraCdcLogMinerWorkerThread logMinerWorkerThread = new OraCdcLogMinerWorkerThread(
          null,
          1000,
          partition,
          0L,
          mineDataSql,
          checkTableSql,
          null,
          1,
          tablesInProcessing,
          Collections.emptySet(),
          SCHEMA_TYPE,
          false,
          false,
          TOPIC,
          odd,
          queuesRoot,
          new HashMap<>(),
          transactionsQueue,
          metrics,
          TOPIC_NAME_STYLE_TABLE,
          TOPIC_NAME_DELIMITER);
      logMinerWorkerThread.start();
      new Thread(() -> {
        while (true) {
          try {
            OraCdcTransaction transaction = transactionsQueue.take();
            LOGGER.info("Start of processing transaction XID {}, first change {}, commit SCN {} statements {}.",
                transaction.getXid(), transaction.getFirstChange(), transaction.getCommitScn(), transaction.length());
            OraCdcLogMinerStatement stmt = new OraCdcLogMinerStatement();
            transaction.getStatement(stmt);
            final OraTable4LogMiner oraTable = tablesInProcessing.get(stmt.getTableId());
            try {
              SourceRecord record = oraTable.parseRedoRecord(stmt);
            } catch (RuntimeException ex) {
              ex.printStackTrace();
            }
          } catch (InterruptedException | SQLException e) {
            e.printStackTrace();
          }
        }
      }).start();
    }
  }

  public static String generateMineDataSql(String objectList) {
    return OraDictSqlTexts.MINE_DATA_NON_CDB + " where (OPERATION_CODE in (1, 2, 3) " + objectList + ") or OPERATION_CODE in (7,36)";
  }

  public static String generateCheckTableSql(String tableList) {
    return OraDictSqlTexts.CHECK_TABLE_NON_CDB + OraDictSqlTexts.CHECK_TABLE_NON_CDB_WHERE_PARAM + tableList;
  }

  public static String generateInitialLoadSql(String tableList) {
    return OraDictSqlTexts.CHECK_TABLE_NON_CDB + tableList;
  }

  public static Map<String, String> getPartition(OraRdbmsInfo rdbmsInfo) {
    final String sourcePartitionName = rdbmsInfo.getInstanceName() + "_" + rdbmsInfo.getHostName();
    LOGGER.debug("Source Partition {} set to {}.", sourcePartitionName, rdbmsInfo.getDbId());
    return Collections.singletonMap(sourcePartitionName, ((Long) rdbmsInfo.getDbId()).toString());
  }

  private static Map<Long, OraTable4LogMiner> buildInitialLoadTableList(
      final String initialLoadSql,
      final OraRdbmsInfo rdbmsInfo,
      final OraDumpDecoder odd,
      final Map<String, String> partition) throws SQLException {
    Map<Long, OraTable4LogMiner> tablesInProcessing = new ConcurrentHashMap<>();
    try (Connection connection = OraPoolConnectionFactory.getConnection();
         PreparedStatement statement = connection.prepareStatement(initialLoadSql);
         ResultSet resultSet = statement.executeQuery()) {
      final boolean isCdb = rdbmsInfo.isCdb();
      while (resultSet.next()) {
        final long objectId = resultSet.getLong("OBJECT_ID");
        final long conId = isCdb ? resultSet.getLong("CON_ID") : 0L;
        final long combinedDataObjectId = (conId << 32) | (objectId & 0xFFFFFFFFL);
        final String tableName = resultSet.getString("TABLE_NAME");
        if (!tablesInProcessing.containsKey(combinedDataObjectId)
            && !StringUtils.startsWith(tableName, "MLOG$_")) {
          OraTable4LogMiner oraTable = new OraTable4LogMiner(
              isCdb ? resultSet.getString("PDB_NAME") : null,
              isCdb ? (short) conId : null,
              resultSet.getString("OWNER"), tableName,
              "ENABLED".equalsIgnoreCase(resultSet.getString("DEPENDENCIES")),
              SCHEMA_TYPE, false, false,
              isCdb, odd, partition, TOPIC, TOPIC_NAME_STYLE_TABLE, TOPIC_NAME_DELIMITER);
          tablesInProcessing.put(combinedDataObjectId, oraTable);
        }
      }
    } catch (SQLException sqle) {
      throw new SQLException(sqle);
    }
    return tablesInProcessing;
  }
}
