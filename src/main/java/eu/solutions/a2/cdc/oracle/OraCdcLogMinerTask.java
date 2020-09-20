/**
 * Copyright (c) 2018-present, A2 Rešitve d.o.o.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See
 * the License for the specific language governing permissions and limitations under the License.
 */

package eu.solutions.a2.cdc.oracle;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import eu.solutions.a2.cdc.oracle.connection.OraDictSqlTexts;
import eu.solutions.a2.cdc.oracle.connection.OraPoolConnectionFactory;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.solutions.a2.cdc.oracle.jmx.OraCdcInitialLoad;
import eu.solutions.a2.cdc.oracle.jmx.OraCdcLogMinerMgmt;
import eu.solutions.a2.cdc.oracle.schema.FileUtils;
import eu.solutions.a2.cdc.oracle.utils.ExceptionUtils;
import eu.solutions.a2.cdc.oracle.utils.OraSqlUtils;
import eu.solutions.a2.cdc.oracle.utils.Version;

/**
 * 
 * @author averemee
 *
 */
public class OraCdcLogMinerTask extends SourceTask {

	private static final Logger LOGGER = LoggerFactory.getLogger(OraCdcLogMinerTask.class);
	private static final int WAIT_FOR_WORKER_MILLIS = 50;

	private int batchSize;
	private int pollInterval;
	private Map<String, String> partition;
	private int schemaType;
	private String topic;
	private int topicNameStyle;
	private String topicNameDelimiter;
	private String stateFileName;
	private OraRdbmsInfo rdbmsInfo;
	private OraCdcLogMinerMgmt metrics;
	private OraDumpDecoder odd;
	private Map<Long, OraTable4LogMiner> tablesInProcessing;
	private Set<Long> tablesOutOfScope;
	private Map<String, OraCdcTransaction> activeTransactions;
	private BlockingQueue<OraCdcTransaction> committedTransactions;
	private OraCdcLogMinerWorkerThread logMinerWorker;
	private OraCdcTransaction transaction;
	private boolean lastStatementInTransaction = true;
	private boolean needToStoreState = false;
	private boolean useOracdcSchemas = false;
	private boolean processLobs = false;
	private CountDownLatch runLatch;
	private AtomicBoolean isPollRunning;
	private boolean execInitialLoad = false;
	private String initialLoadStatus = ParamConstants.INITIAL_LOAD_IGNORE;
	private OraCdcInitialLoadThread initialLoadWorker;
	private BlockingQueue<OraTable4InitialLoad> tablesQueue;
	private OraTable4InitialLoad table4InitialLoad;
	private boolean lastRecordInTable = true;
	private OraCdcInitialLoad initialLoadMetrics;

	@Override
	public String version() {
		return Version.getVersion();
	}

	@Override
	public void start(Map<String, String> props) {
		LOGGER.info("Starting oracdc logminer source task");

		batchSize = Integer.parseInt(props.get(ParamConstants.BATCH_SIZE_PARAM));
		LOGGER.debug("batchSize = {} records.", batchSize);
		pollInterval = Integer.parseInt(props.get(ParamConstants.POLL_INTERVAL_MS_PARAM));
		LOGGER.debug("pollInterval = {} ms.", pollInterval);
		schemaType = Integer.parseInt(props.get(ParamConstants.SCHEMA_TYPE_PARAM));
		LOGGER.debug("schemaType (Integer value 1 for Debezium, 2 for Kafka STD) = {} .", schemaType);
		if (schemaType == ParamConstants.SCHEMA_TYPE_INT_KAFKA_STD) {
			topic = props.get(OraCdcSourceConnectorConfig.TOPIC_PREFIX_PARAM);
			switch (props.get(ParamConstants.TOPIC_NAME_STYLE_PARAM)) {
			case ParamConstants.TOPIC_NAME_STYLE_TABLE:
				topicNameStyle = ParamConstants.TOPIC_NAME_STYLE_INT_TABLE;
				break;
			case ParamConstants.TOPIC_NAME_STYLE_SCHEMA_TABLE:
				topicNameStyle = ParamConstants.TOPIC_NAME_STYLE_INT_SCHEMA_TABLE;
				break;
			case ParamConstants.TOPIC_NAME_STYLE_PDB_SCHEMA_TABLE:
				topicNameStyle = ParamConstants.TOPIC_NAME_STYLE_INT_PDB_SCHEMA_TABLE;
				break;
			}
			topicNameDelimiter = props.get(ParamConstants.TOPIC_NAME_DELIMITER_PARAM);
		} else {
			// ParamConstants.SCHEMA_TYPE_INT_DEBEZIUM
			topic = props.get(OraCdcSourceConnectorConfig.KAFKA_TOPIC_PARAM);
		}
		LOGGER.debug("topic set to {}.", topic);
		useOracdcSchemas = Boolean.parseBoolean(props.get(ParamConstants.ORACDC_SCHEMAS_PARAM));
		if (useOracdcSchemas) {
			LOGGER.debug("oracdc will use own schemas for Oracle NUMBER and TIMESTAMP WITH [LOCAL] TIMEZONE datatypes");
		}
		//TODO
		//TODO parseLobs!!!
		//TODO

		try (Connection connDictionary = OraPoolConnectionFactory.getConnection()) {
			rdbmsInfo = OraRdbmsInfo.getInstance();
			odd = new OraDumpDecoder(rdbmsInfo.getDbCharset(), rdbmsInfo.getDbNCharCharset());
			metrics = new OraCdcLogMinerMgmt(rdbmsInfo, props.get("name"), this);

			final String sourcePartitionName = rdbmsInfo.getInstanceName() + "_" + rdbmsInfo.getHostName();
			LOGGER.debug("Source Partition {} set to {}.", sourcePartitionName, rdbmsInfo.getDbId());
			partition = Collections.singletonMap(sourcePartitionName, ((Long)rdbmsInfo.getDbId()).toString());

			final Long redoSizeThreshold;
			final Integer redoFilesCount;
			if (props.containsKey(ParamConstants.REDO_FILES_SIZE_PARAM)) {
				redoSizeThreshold = Long.parseLong(props.get(ParamConstants.REDO_FILES_SIZE_PARAM));
				redoFilesCount = null;
			} else {
				redoSizeThreshold = null;
				redoFilesCount = Integer.parseInt(props.get(ParamConstants.REDO_FILES_COUNT_PARAM));
			}

			List<String> excludeList = null;
			List<String> includeList = null;
			if (props.containsKey(ParamConstants.TABLE_EXCLUDE_PARAM)) {
				excludeList =
						Arrays.asList(props.get(ParamConstants.TABLE_EXCLUDE_PARAM).split("\\s*,\\s*"));
			}
			if (props.containsKey(ParamConstants.TABLE_INCLUDE_PARAM)) {
				includeList =
						Arrays.asList(props.get(ParamConstants.TABLE_INCLUDE_PARAM).split("\\s*,\\s*"));
			}

			final Path queuesRoot = FileSystems.getDefault().getPath(
					props.get(ParamConstants.TEMP_DIR_PARAM));

			if (useOracdcSchemas) {
				// Use stored schema only in this mode
				final String schemaFileName = props.get(ParamConstants.DICTIONARY_FILE_PARAM);
				if (!StringUtils.isEmpty(schemaFileName)) {
					try {
						LOGGER.info("Loading stored schema definitions from file {}.", schemaFileName);
						tablesInProcessing = FileUtils.readDictionaryFile(schemaFileName, schemaType);
						LOGGER.info("{} table schema definitions loaded from file {}.",
								tablesInProcessing.size(), schemaFileName);
						tablesInProcessing.forEach((key, table) -> {
							table.setTopicDecoderPartition(
									topic, topicNameStyle, topicNameDelimiter, odd, partition);
							metrics.addTableInProcessing(table.fqn());
						});
					} catch (IOException ioe) {
						LOGGER.warn("Unable to read stored definition from {}.", schemaFileName);
						LOGGER.warn(ExceptionUtils.getExceptionStackTrace(ioe));
					}
				}
			}
			if (tablesInProcessing == null) {
				tablesInProcessing = new ConcurrentHashMap<>();
			}
			tablesOutOfScope = new HashSet<>();
			activeTransactions = new HashMap<>();
			committedTransactions = new LinkedBlockingQueue<>();

			boolean rewind = false;
			final long firstScn;
			String firstRsId = null;
			long firstSsn = 0;
			final boolean startScnFromProps = props.containsKey(ParamConstants.LGMNR_START_SCN_PARAM);
			stateFileName = props.get(ParamConstants.PERSISTENT_STATE_FILE_PARAM);
			final Path stateFilePath = Paths.get(stateFileName);
			// Initial load
			if (ParamConstants.INITIAL_LOAD_EXECUTE.equals(props.get(ParamConstants.INITIAL_LOAD_PARAM))) {
				execInitialLoad = true;
				initialLoadStatus = ParamConstants.INITIAL_LOAD_EXECUTE;
			}
			if (stateFilePath.toFile().exists()) {
				// File with stored state exists
				final long restoreStarted = System.currentTimeMillis();
				OraCdcPersistentState persistentState = OraCdcPersistentState.fromFile(stateFileName);
				LOGGER.info("Will start processing using stored persistent state file {} dated {}.",
						stateFileName,
						LocalDateTime.ofInstant(
								Instant.ofEpochMilli(persistentState.getLastOpTsMillis()), ZoneId.systemDefault()
							).format(DateTimeFormatter.ISO_DATE_TIME));
				if (rdbmsInfo.getDbId() != persistentState.getDbId()) {
					LOGGER.error("DBID from stored state file {} and from connection {} are different!",
							persistentState.getDbId(), rdbmsInfo.getDbId());
					LOGGER.error("Exiting.");
					throw new ConnectException("Unable to use stored file for database with different DBID!!!");
				}
				LOGGER.debug(persistentState.toString());
				// Begin - initial load analysis...
				if (execInitialLoad) {
					// Need to check state file value
					final String initialLoadFromStateFile = persistentState.getInitialLoad();
					if (ParamConstants.INITIAL_LOAD_COMPLETED.equals(initialLoadFromStateFile)) {
						execInitialLoad = false;
						initialLoadStatus = ParamConstants.INITIAL_LOAD_COMPLETED;
						LOGGER.info("Initial load set to {} (value from state file)", ParamConstants.INITIAL_LOAD_COMPLETED);
					}
				}

				// End - initial load analysis...
				if (startScnFromProps) {
					// a2.first.change set in parameters, ignore stored state, rename file
					firstScn = Long.parseLong(props.get(ParamConstants.LGMNR_START_SCN_PARAM));
					LOGGER.info("Ignoring last processed SCN value from stored state file {} and setting it to {} from connector properties",
							stateFileName, firstScn);
				} else {
					firstScn = persistentState.getLastScn();
					firstRsId = persistentState.getLastRsId();
					firstSsn = persistentState.getLastSsn();
					if (persistentState.getCurrentTransaction() != null) {
						transaction = OraCdcTransaction.restoreFromMap(persistentState.getCurrentTransaction());
						// To prevent committedTransactions.poll() in this.poll()
						lastStatementInTransaction = false;
						LOGGER.debug("Restored current transaction {}", transaction.toString());
					}
					if (persistentState.getCommittedTransactions() != null) {
						for (int i = 0; i < persistentState.getCommittedTransactions().size(); i++) {
							final OraCdcTransaction oct = OraCdcTransaction.restoreFromMap(
									persistentState.getCommittedTransactions().get(i));
							committedTransactions.add(oct);
							LOGGER.debug("Restored committed transaction {}", oct.toString());
						}
					}
					if (persistentState.getInProgressTransactions() != null) {
						for (int i = 0; i < persistentState.getInProgressTransactions().size(); i++) {
							final OraCdcTransaction oct = OraCdcTransaction.restoreFromMap(
									persistentState.getInProgressTransactions().get(i));
							activeTransactions.put(oct.getXid(), oct);
							LOGGER.debug("Restored in progress transaction {}", oct.toString());
						}
					}
					if (persistentState.getProcessedTablesIds() != null) {
						restoreTableInfoFromDictionary(persistentState.getProcessedTablesIds());
					}
					if (persistentState.getOutOfScopeTablesIds() != null) {
						persistentState.getOutOfScopeTablesIds().forEach(combinedId -> {
							tablesOutOfScope.add(combinedId);
							if (LOGGER.isDebugEnabled()) {
								final int tableId = (int) ((long) combinedId);
								final int conId = (int) (combinedId >> 32);
								LOGGER.debug("Restored out of scope table OBJECT_ID {} from CON_ID {}", tableId, conId);
							}
						});
					}
					LOGGER.info("Restore persistent state {} ms", (System.currentTimeMillis() - restoreStarted));
					rewind = true;
				}
				final String savedStateFile = stateFileName + "." + System.currentTimeMillis(); 
				Files.copy(stateFilePath, Paths.get(savedStateFile), StandardCopyOption.REPLACE_EXISTING);
				LOGGER.info("Stored state file {} copied to {}", stateFileName, savedStateFile);
			} else {
				if (startScnFromProps) {
					firstScn = Long.parseLong(props.get(ParamConstants.LGMNR_START_SCN_PARAM));
					LOGGER.info("Using first SCN value {} from connector properties.", firstScn);
				} else {
					firstScn = OraRdbmsInfo.firstScnFromArchivedLogs(OraPoolConnectionFactory.getLogMinerConnection());
					LOGGER.info("Using min(FIRST_CHANGE#) from V$ARCHIVED_LOG = {} as first SCN value.", firstScn);
				}
			}

			String checkTableSql = null;
			String mineDataSql = null;
			String initialLoadSql = null;
			if (rdbmsInfo.isCdb()) {
				mineDataSql = OraDictSqlTexts.MINE_DATA_CDB;
				checkTableSql = OraDictSqlTexts.CHECK_TABLE_CDB + OraDictSqlTexts.CHECK_TABLE_CDB_WHERE_PARAM;
				if (execInitialLoad) {
					initialLoadSql = OraDictSqlTexts.CHECK_TABLE_CDB;
				}
			} else {
				mineDataSql = OraDictSqlTexts.MINE_DATA_NON_CDB;
				checkTableSql = OraDictSqlTexts.CHECK_TABLE_NON_CDB + OraDictSqlTexts.CHECK_TABLE_NON_CDB_WHERE_PARAM;
				if (execInitialLoad) {
					initialLoadSql = OraDictSqlTexts.CHECK_TABLE_NON_CDB;
				}
			}
			if (includeList != null) {
				final String tableList = OraSqlUtils.parseTableSchemaList(
						false, OraSqlUtils.MODE_WHERE_ALL_OBJECTS, includeList);
				final String objectList = rdbmsInfo.getMineObjectsIds(
						connDictionary, false, tableList);
				if (StringUtils.endsWith(objectList, "()")) {
					// and DATA_OBJ# in ()
					LOGGER.error("{} parameter set to {} but there are no tables matching this condition.\nExiting.",
							ParamConstants.TABLE_INCLUDE_PARAM, props.get(ParamConstants.TABLE_INCLUDE_PARAM));
					throw new ConnectException("Please check value of a2.include parameter or remove it from configuration!");
				}
				mineDataSql += "where ((OPERATION_CODE in (1,2,3) " +  objectList + ")";
				checkTableSql += tableList;
				if (execInitialLoad) {
					initialLoadSql += tableList;
				}
			}
			if (excludeList != null) {
				if (includeList != null) {
					mineDataSql += " and (OPERATION_CODE in (1,2,3) ";
				} else {
					mineDataSql += " where ((OPERATION_CODE in (1,2,3) ";
				}
				final String objectList = rdbmsInfo.getMineObjectsIds(connDictionary, true,
						OraSqlUtils.parseTableSchemaList(false, OraSqlUtils.MODE_WHERE_ALL_OBJECTS, excludeList));
				if (StringUtils.endsWith(objectList, "()")) {
					// and DATA_OBJ# not in ()
					LOGGER.error("{} parameter set to {} but there are no tables matching this condition.\nExiting.",
							ParamConstants.TABLE_EXCLUDE_PARAM, props.get(ParamConstants.TABLE_EXCLUDE_PARAM));
					throw new ConnectException("Please check value of a2.exclude parameter or remove it from configuration!");
				}
				mineDataSql += objectList + ")";
				final String tableList = OraSqlUtils.parseTableSchemaList(true, OraSqlUtils.MODE_WHERE_ALL_OBJECTS, excludeList);
				checkTableSql += tableList;
				if (execInitialLoad) {
					initialLoadSql += tableList;
				}
			}
			if (includeList == null && excludeList == null) {
				mineDataSql += "where (OPERATION_CODE in (1,2,3) ";
			}
			// Finally - COMMIT and ROLLBACK
			mineDataSql += " or OPERATION_CODE in (7,36))";
			if (rdbmsInfo.isCdb()) {
				// Do not process objects from CDB$ROOT and PDB$SEED
				mineDataSql += rdbmsInfo.getConUidsList(OraPoolConnectionFactory.getLogMinerConnection());
			}
			LOGGER.debug("Mining SQL = {}", mineDataSql);
			LOGGER.debug("Dictionary check SQL = {}", checkTableSql);
			if (execInitialLoad) {
				LOGGER.debug("Initial load table list SQL {}", initialLoadSql);
				tablesQueue = new LinkedBlockingQueue<>();
				buildInitialLoadTableList(initialLoadSql);
				initialLoadMetrics = new OraCdcInitialLoad(rdbmsInfo, props.get("name"));
				initialLoadWorker = new OraCdcInitialLoadThread(
						WAIT_FOR_WORKER_MILLIS,
						firstScn,
						tablesInProcessing,
						queuesRoot,
						rdbmsInfo,
						initialLoadMetrics,
						tablesQueue);
			}

			logMinerWorker = new OraCdcLogMinerWorkerThread(
					this,
					pollInterval,
					partition,
					firstScn,
					mineDataSql,
					checkTableSql,
					redoSizeThreshold,
					redoFilesCount,
					tablesInProcessing,
					tablesOutOfScope,
					schemaType,
					useOracdcSchemas,
					processLobs,
					topic,
					odd,
					queuesRoot,
					activeTransactions,
					committedTransactions,
					metrics,
					topicNameStyle,
					topicNameDelimiter);
			if (rewind) {
				logMinerWorker.rewind(firstScn, firstRsId, firstSsn);
			}

		} catch (SQLException | InvalidPathException | IOException e) {
			LOGGER.error("Unable to start oracdc logminer task!");
			LOGGER.error(ExceptionUtils.getExceptionStackTrace(e));
			throw new ConnectException(e);
		}
		LOGGER.trace("Starting worker thread.");
		if (execInitialLoad) {
			initialLoadWorker.start();
		}
		logMinerWorker.start();
		needToStoreState = true;
		runLatch = new CountDownLatch(1);
		isPollRunning = new AtomicBoolean(false);
	}

	@Override
	public List<SourceRecord> poll() throws InterruptedException {
		LOGGER.trace("BEGIN: poll()");
		if (runLatch.getCount() < 1) {
			LOGGER.trace("Returning from poll() -> processing stopped");
			isPollRunning.set(false);
			return null;
		}
		isPollRunning.set(true);
		List<SourceRecord> result = new ArrayList<>();
		if (execInitialLoad) {
			// Execute initial load...
			if (!initialLoadWorker.isRunning() && tablesQueue.isEmpty() && table4InitialLoad == null) {
				Thread.sleep(WAIT_FOR_WORKER_MILLIS);
				if (tablesQueue.isEmpty()) {
					LOGGER.info("Initial load completed");
					execInitialLoad = false;
					initialLoadStatus = ParamConstants.INITIAL_LOAD_COMPLETED;
					return null;
				}
			}
			int recordCount = 0;
			while (recordCount < batchSize) {
				if (lastRecordInTable) {
					//First table or end of table reached, need to poll new
					table4InitialLoad = tablesQueue.poll();
					if (table4InitialLoad != null) {
						initialLoadMetrics.startSendTable(table4InitialLoad.fqn());
						LOGGER.info("Table {} initial load (send to Kafka phase) started.",
								table4InitialLoad.fqn());
					}
				}
				if (table4InitialLoad == null) {
					LOGGER.debug("Waiting {} ms for initial load data...", pollInterval);
					Thread.sleep(pollInterval);
					break;
				} else {
					lastRecordInTable = false;
					// Processing.......
					SourceRecord record = table4InitialLoad.getSourceRecord();
					if (record == null) {
						initialLoadMetrics.finishSendTable(table4InitialLoad.fqn());
						LOGGER.info("Table {} initial load (send to Kafka phase) completed.",
								table4InitialLoad.fqn());
						lastRecordInTable = true;
						table4InitialLoad.close();
						table4InitialLoad = null;
					} else {
						result.add(record);
						recordCount++;
					}
				}
			}
		} else {
			// Load data from archived redo...
			int recordCount = 0;
			int parseTime = 0;
			while (recordCount < batchSize) {
				if (lastStatementInTransaction) {
					// End of transaction, need to poll new
					transaction = committedTransactions.poll();
				}
				if (transaction == null) {
					// No more records produced by LogMiner worker
					break;
				} else {
					// Prepare records...
					if (LOGGER.isDebugEnabled()) {
						LOGGER.debug("Start of processing transaction XID {}, first change {}, commit SCN {}.",
							transaction.getXid(), transaction.getFirstChange(), transaction.getCommitScn());
					}
					lastStatementInTransaction = false;
					boolean processTransaction = true;
					do {
						OraCdcLogMinerStatement stmt = new OraCdcLogMinerStatement();
						processTransaction = transaction.getStatement(stmt);
						lastStatementInTransaction = !processTransaction;

						if (processTransaction) {
							final OraTable4LogMiner oraTable = tablesInProcessing.get(stmt.getTableId());
							if (oraTable == null) {
								LOGGER.error("Strange consistency issue for DATA_OBJ# {}, transaction XID {}, statement SCN={}, RS_ID='{}', SSN={}.\n Exiting.",
										stmt.getTableId(), transaction.getXid(), stmt.getScn(), stmt.getRsId(), stmt.getSsn());
								isPollRunning.set(false);
								throw new ConnectException("Strange consistency issue!!!");
							} else {
								try {
									final long startParseTs = System.currentTimeMillis();
									SourceRecord record = oraTable.parseRedoRecord(stmt);
									result.add(record);
									recordCount++;
									parseTime += (System.currentTimeMillis() - startParseTs);
								} catch (SQLException e) {
									LOGGER.error(e.getMessage());
									LOGGER.error(ExceptionUtils.getExceptionStackTrace(e));
									isPollRunning.set(false);
									throw new ConnectException(e);
								}
							}
						}
					} while (processTransaction && recordCount < batchSize);
					if (lastStatementInTransaction) {
						// close Cronicle queue only when all statements are processed
						if (LOGGER.isDebugEnabled()) {
							LOGGER.debug("End of processing transaction XID {}, first change {}, commit SCN {}.",
								transaction.getXid(), transaction.getFirstChange(), transaction.getCommitScn());
						}
						transaction.close();
						transaction = null;
					}
				}
			}
			if (recordCount == 0) {
				synchronized (this) {
					LOGGER.debug("Waiting {} ms", pollInterval);
					Thread.sleep(pollInterval);
				}
			} else {
				metrics.addSentRecords(result.size(), parseTime);
			}
		}
		isPollRunning.set(false);
		LOGGER.trace("END: poll()");
		return result;
	}

	@Override
	public void stop() {
		stop(true);
	}

	public void stop(boolean stopWorker) {
		LOGGER.info("Stopping oracdc logminer source task.");
		if (runLatch != null ) {
			// We can stop before runLatch initialization due to invalid parameters
			runLatch.countDown();
			if (stopWorker) {
				logMinerWorker.shutdown();
				while (logMinerWorker.isRunning()) {
					try {
						LOGGER.debug("Waiting {} ms for worker thread to stop...", WAIT_FOR_WORKER_MILLIS);
						Thread.sleep(WAIT_FOR_WORKER_MILLIS);
					} catch (InterruptedException e) {
						LOGGER.error(ExceptionUtils.getExceptionStackTrace(e));
					}
				}
			} else {
				while (isPollRunning.get()) {
					try {
						LOGGER.debug("Waiting {} ms for connector task to stop...", WAIT_FOR_WORKER_MILLIS);
						Thread.sleep(WAIT_FOR_WORKER_MILLIS);
					} catch (InterruptedException e) {
						LOGGER.error(ExceptionUtils.getExceptionStackTrace(e));
					}
				}
			}
			if (needToStoreState) {
				try {
					saveState(true);
				} catch(IOException ioe) {
					LOGGER.error("Unable to save state to file " + stateFileName + "!");
					LOGGER.error(ExceptionUtils.getExceptionStackTrace(ioe));
					throw new ConnectException("Unable to save state to file " + stateFileName + "!");
			}

			} else {
				LOGGER.info("Do not need to run store state procedures.");
				LOGGER.info("Check Connect log files for errors.");
			}
		}
	}

	/**
	 * 
	 * @param saveFinalState     when set to true performs full save, when set to false only
	 *                           in-progress transactions are saved
	 * @throws IOException
	 */
	public void saveState(boolean saveFinalState) throws IOException {
		final long saveStarted = System.currentTimeMillis();
		final String fileName = saveFinalState ?
				stateFileName : (stateFileName + "-jmx-" + System.currentTimeMillis());
		LOGGER.info("Saving oracdc state to {} file...", fileName);
		OraCdcPersistentState ops = new OraCdcPersistentState();
		ops.setDbId(rdbmsInfo.getDbId());
		ops.setInstanceName(rdbmsInfo.getInstanceName());
		ops.setHostName(rdbmsInfo.getHostName());
		ops.setLastOpTsMillis(System.currentTimeMillis());
		ops.setLastScn(logMinerWorker.getLastScn());
		ops.setLastRsId(logMinerWorker.getLastRsId());
		ops.setLastSsn(logMinerWorker.getLastSsn());
		ops.setInitialLoad(initialLoadStatus);
		if (saveFinalState) {
			if (transaction != null) {
				ops.setCurrentTransaction(transaction.attrsAsMap());
				LOGGER.debug("Added to state file transaction {}", transaction.toString());
			}
			if (!committedTransactions.isEmpty()) {
				final List<Map<String, Object>> committed = new ArrayList<>();
				committedTransactions.stream().forEach(trans -> {
					committed.add(trans.attrsAsMap());
					LOGGER.debug("Added to state file committed transaction {}", trans.toString());
				});
				ops.setCommittedTransactions(committed);
			}
		}
		if (!activeTransactions.isEmpty()) {
			final List<Map<String, Object>> wip = new ArrayList<>();
			activeTransactions.forEach((xid, trans) -> {
				wip.add(trans.attrsAsMap());
				LOGGER.debug("Added to state file in progress transaction {}", trans.toString());
			});
			ops.setInProgressTransactions(wip);
		}
		if (!tablesInProcessing.isEmpty()) {
			final List<Long> wipTables = new ArrayList<>();
			tablesInProcessing.forEach((combinedId, table) -> {
				wipTables.add(combinedId);
				if (LOGGER.isDebugEnabled()) {
					final int tableId = (int) ((long) combinedId);
					final int conId = (int) (combinedId >> 32);
					LOGGER.debug("Added to state file in process table OBJECT_ID {} from CON_ID {}", tableId, conId);
				}
			});
			ops.setProcessedTablesIds(wipTables);
		}
		if (!tablesOutOfScope.isEmpty()) {
			final List<Long> oosTables = new ArrayList<>();
			tablesOutOfScope.forEach(combinedId -> {
				oosTables.add(combinedId);
				metrics.addTableOutOfScope();
				if (LOGGER.isDebugEnabled()) {
					final int tableId = (int) ((long) combinedId);
					final int conId = (int) (combinedId >> 32);
					LOGGER.debug("Added to state file in out of scope table OBJECT_ID {} from CON_ID {}", tableId, conId);
				}
			});
			ops.setOutOfScopeTablesIds(oosTables);
		}
		try {
			ops.toFile(fileName);
		} catch (Exception e) {
			LOGGER.error("Unable to save state file with contents:\n{}", ops.toString());
			throw new IOException(e);
		}
		LOGGER.info("oracdc state saved to {} file, elapsed {} ms",
				fileName, (System.currentTimeMillis() - saveStarted));
		LOGGER.debug("State file contents:\n{}", ops.toString());
	}

	public void saveTablesSchema() throws IOException {
		String schemaFileName = null;
		try {
			schemaFileName = stateFileName.substring(0, stateFileName.lastIndexOf(File.separator));
		} catch (Exception e) {
			LOGGER.error("Unable to detect parent directory for {} using {} separator.",
					stateFileName, File.separator);
			schemaFileName = System.getProperty("java.io.tmpdir");
		}
		schemaFileName += File.separator + "oracdc.schemas-" + System.currentTimeMillis();

		FileUtils.writeDictionaryFile(tablesInProcessing, schemaFileName);
	}

	private void restoreTableInfoFromDictionary(List<Long> processedTablesIds) throws SQLException {
		//TODO
		//TODO Same code as in WorkerThread - require serious improvement!!!
		//TODO
		final Connection connection = OraPoolConnectionFactory.getConnection();
		final PreparedStatement psCheckTable;
		final boolean isCdb = rdbmsInfo.isCdb();
		if (isCdb) {
			psCheckTable = connection.prepareStatement(
					OraDictSqlTexts.CHECK_TABLE_CDB + OraDictSqlTexts.CHECK_TABLE_CDB_WHERE_PARAM,
					ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
		} else {
			psCheckTable = connection.prepareStatement(
					OraDictSqlTexts.CHECK_TABLE_NON_CDB + OraDictSqlTexts.CHECK_TABLE_NON_CDB_WHERE_PARAM,
					ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
		}
		for (long combinedDataObjectId : processedTablesIds) {
			if (!tablesInProcessing.containsKey(combinedDataObjectId)) {
				final int tableId = (int) combinedDataObjectId;
				final int conId = (int) (combinedDataObjectId >> 32);
				psCheckTable.setInt(1, tableId);
				if (isCdb) {
					psCheckTable.setInt(2, conId);
				}
				LOGGER.debug("Adding from database dictionary for internal id {}: OBJECT_ID = {}, CON_ID = {}",
						combinedDataObjectId, tableId, conId);
				final ResultSet rsCheckTable = psCheckTable.executeQuery();
				if (rsCheckTable.next()) {
					final String tableName = rsCheckTable.getString("TABLE_NAME");
					final String tableOwner = rsCheckTable.getString("OWNER");
					OraTable4LogMiner oraTable = new OraTable4LogMiner(
							isCdb ? rsCheckTable.getString("PDB_NAME") : null,
							isCdb ? (short) conId : null,
							tableOwner, tableName,
							"ENABLED".equalsIgnoreCase(rsCheckTable.getString("DEPENDENCIES")),
							schemaType, useOracdcSchemas, processLobs,
							isCdb, odd, partition, topic, topicNameStyle, topicNameDelimiter);
					tablesInProcessing.put(combinedDataObjectId, oraTable);
					metrics.addTableInProcessing(oraTable.fqn());
					LOGGER.debug("Restored metadata for table {}, OBJECT_ID={}, CON_ID={}",
							oraTable.fqn(), tableId, conId);
				} else {
					throw new SQLException("Data corruption detected!\n" +
							"OBJECT_ID=" + tableId + ", CON_ID=" + conId + 
							" exist in stored state but not in database!!!");
				}
				rsCheckTable.close();
				psCheckTable.clearParameters();
			}
		}
		psCheckTable.close();
	}

	private void buildInitialLoadTableList(final String initialLoadSql) throws SQLException {
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
							schemaType, useOracdcSchemas, processLobs,
							isCdb, odd, partition, topic, topicNameStyle, topicNameDelimiter);
					tablesInProcessing.put(combinedDataObjectId, oraTable);
				}
			}
		} catch (SQLException sqle) {
			throw new SQLException(sqle);
		}
	}

}