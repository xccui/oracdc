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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;

import eu.solutions.a2.cdc.oracle.connection.OraDictSqlTexts;
import eu.solutions.a2.cdc.oracle.connection.OraPoolConnectionFactory;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

/**
 * 
 * @author averemee
 *
 */
public class OraRdbmsInfo {

	private final String versionString;
	private final int versionMajor;
	private final short instanceNumber;
	private final String instanceName;
	private final String hostName;
	private final int cpuCoreCount;
	private final long dbId;
	private final String databaseName;
	private final String platformName;
	private final boolean cdb;
	private final boolean cdbRoot;
	private final Schema schema;
	private final String dbCharset;
	private final String dbNCharCharset;
	private final String dbUniqueName;

	private final static int CDB_INTRODUCED = 12;

	private static OraRdbmsInfo instance;

	public OraRdbmsInfo(final Connection connection) throws SQLException {
		this(connection, true);
	}

	private OraRdbmsInfo(final Connection connection, final boolean initialCall) throws SQLException {
		PreparedStatement ps = connection.prepareStatement(OraDictSqlTexts.RDBMS_VERSION_AND_MORE,
				ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
		ResultSet rs = ps.executeQuery();
		if (rs.next()) {
			versionString = rs.getString("VERSION");
			instanceNumber = rs.getShort("INSTANCE_NUMBER");
			instanceName = rs.getString("INSTANCE_NAME");
			hostName = rs.getString("HOST_NAME");
			cpuCoreCount = rs.getInt("CPU_CORE_COUNT_CURRENT");
		} else {
			throw new SQLException("Unable to detect RDBMS version!");
		}
		rs.close();
		rs = null;
		ps.close();
		ps = null;

		versionMajor = Integer.parseInt(
				versionString.substring(0, versionString.indexOf(".")));

		if (versionMajor < CDB_INTRODUCED) {
			cdb = false;
			cdbRoot = false;
			ps = connection.prepareStatement(OraDictSqlTexts.DB_INFO_PRE12C,
					ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
			rs = ps.executeQuery();
			if (!rs.next()) {
				throw new SQLException("Unable to detect database information!");
			}
		} else {
			ps = connection.prepareStatement(OraDictSqlTexts.DB_CDB_PDB_INFO,
					ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
			rs = ps.executeQuery();
			if (rs.next()) {
				if ("YES".equalsIgnoreCase(rs.getString("CDB"))) {
					cdb = true;
					if ("CDB$ROOT".equalsIgnoreCase(rs.getString("CON_NAME"))) {
						cdbRoot = true;
					} else {
						cdbRoot = false;
					}
				} else {
					cdb = false;
					cdbRoot = false;
				}
			} else
				throw new SQLException("Unable to detect CDB/PDB status!");
		}

		dbId = rs.getLong("DBID");
		databaseName = rs.getString("NAME");
		dbUniqueName = rs.getString("DB_UNIQUE_NAME");
		platformName = rs.getString("PLATFORM_NAME");
		dbCharset = rs.getString("NLS_CHARACTERSET");
		dbNCharCharset = rs.getString("NLS_NCHAR_CHARACTERSET");
		rs.close();
		rs = null;
		ps.close();
		ps = null;

		SchemaBuilder schemaBuilder = SchemaBuilder
				.struct()
				.name("eu.solutions.a2.cdc.oracle.Source");
		schemaBuilder.field("instance_number", Schema.INT16_SCHEMA);
		schemaBuilder.field("version", Schema.STRING_SCHEMA);
		schemaBuilder.field("instance_name", Schema.STRING_SCHEMA);
		schemaBuilder.field("host_name", Schema.STRING_SCHEMA);
		schemaBuilder.field("dbid", Schema.INT64_SCHEMA);
		schemaBuilder.field("database_name", Schema.STRING_SCHEMA);
		schemaBuilder.field("platform_name", Schema.STRING_SCHEMA);
		// Table/Operation specific
		schemaBuilder.field("query", Schema.OPTIONAL_STRING_SCHEMA);
		schemaBuilder.field("pdb_name", Schema.OPTIONAL_STRING_SCHEMA);
		schemaBuilder.field("owner", Schema.OPTIONAL_STRING_SCHEMA);
		schemaBuilder.field("table", Schema.OPTIONAL_STRING_SCHEMA);
		schemaBuilder.field("scn", Schema.INT64_SCHEMA);
		schemaBuilder.field("ts_ms", Schema.INT64_SCHEMA);
		schema = schemaBuilder.build();

		// It's No Good...
		if (instance == null && initialCall)
			instance = this;
	}

	public static OraRdbmsInfo getInstance() throws SQLException {
		// It's No Good...
		if (instance == null) {
			final Connection connection = OraPoolConnectionFactory.getConnection();
			instance = new OraRdbmsInfo(connection, false);
			connection.close();
		}
		return instance;
	}

	public Struct getStruct(String query, String pdbName, String owner, String table, long scn, Long ts) {
		Struct struct = new Struct(schema);
		struct.put("instance_number", instanceNumber);
		struct.put("version", versionString);
		struct.put("instance_name", instanceName);
		struct.put("host_name", hostName);
		struct.put("dbid", dbId);
		struct.put("database_name", databaseName);
		struct.put("platform_name", platformName);
		// Table/Operation specific
		if (query != null)
			struct.put("query", query);
		if (pdbName != null)
			struct.put("pdb_name", pdbName);
		if (owner != null)
			struct.put("owner", owner);
		if (table != null)
			struct.put("table", table);
		struct.put("scn", scn);
		if (ts != null)
			struct.put("ts_ms", ts);
		else
			struct.put("ts_ms", 0l);
		return struct;
	}

	/**
	 * Returns set of column names for primary key or it equivalent (unique with all non-null)
	 * 
	 * @param connection - Connection to data dictionary (db in 'OPEN' state)
	 * @param conId      - CON_ID, if null we working with non CDB or pre-12c Oracle Database
	 * @param owner      - Table owner
	 * @param tableName  - Table name
	 * @return           - Set with names of primary key columns. null if nothing found
	 * @throws SQLException
	 */
	public static Set<String> getPkColumnsFromDict(
			final Connection connection,
			final Short conId,
			final String owner,
			final String tableName) throws SQLException {
		Set<String> result = null;
		PreparedStatement ps = connection.prepareStatement(
				(conId == null) ?
						OraDictSqlTexts.WELL_DEFINED_PK_COLUMNS_NON_CDB :
						OraDictSqlTexts.WELL_DEFINED_PK_COLUMNS_CDB,
				ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
		ps.setString(1, owner);
		ps.setString(2, tableName);
		if (conId != null) {
			ps.setShort(3, conId);			
		}

		ResultSet rs = ps.executeQuery();
		while (rs.next()) {
			if (result == null)
				result = new HashSet<>();
			result.add(rs.getString("COLUMN_NAME"));
		}
		rs.close(); rs = null;
		ps.close(); ps = null;
		if (result == null) {
			// Try to find unique index with non-null columns only
			ps = connection.prepareStatement(
					(conId == null) ?
							OraDictSqlTexts.LEGACY_DEFINED_PK_COLUMNS_NON_CDB :
							OraDictSqlTexts.LEGACY_DEFINED_PK_COLUMNS_CDB,
					ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
			ps.setString(1, owner);
			ps.setString(2, tableName);
			if (conId != null) {
				ps.setShort(3, conId);			
			}
			rs = ps.executeQuery();
			while (rs.next()) {
				if (result == null)
					result = new HashSet<>();
				result.add(rs.getString("COLUMN_NAME"));
			}
			rs.close(); rs = null;
			ps.close(); ps = null;
		}
		return result;
	}

	/**
	 * Returns first available SCN from V$ARCHIVED_LOG
	 * 
	 * @param connection - Connection to mining database
	 * @return           - first available SCN
	 * @throws SQLException
	 */
	public static long firstScnFromArchivedLogs(final Connection connection) throws SQLException {
		PreparedStatement ps = connection.prepareStatement(OraDictSqlTexts.FIRST_AVAILABLE_SCN_IN_ARCHIVE,
				ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
		long firstScn = -1;
		ResultSet rs = ps.executeQuery();
		if (rs.next())
			firstScn = rs.getLong(1);
		else
			throw new SQLException("Something wrong with access to V$ARCHIVED_LOG or no archived log exists!");
		rs.close(); rs = null;
		ps.close(); ps = null;
		return firstScn;
	}

	/**
	 * Returns part of WHERE with OBJECT_ID's to exclude or include
	 * 
	 * @param isCdb
	 * @param connection - Connection to dictionary database
	 * @param exclude
	 * @param where
	 * @return
	 * @throws SQLException
	 */
	public String getMineObjectsIds(final Connection connection,
			final boolean exclude, final String where) throws SQLException {
		final StringBuilder sb = new StringBuilder(32768);
		if (exclude) {
			sb.append(" and DATA_OBJ# not in (");
		} else {
			sb.append(" and DATA_OBJ# in (");
		}
		//TODO
		//TODO For CDB - pair required!!!
		//TODO OBJECT_ID is not unique!!!
		//TODO Will work well obly with 
		//TODO
		PreparedStatement ps = connection.prepareStatement(
				"select OBJECT_ID\n" +
				(this.cdb ? "from   CDB_OBJECTS O\n" : "from   DBA_OBJECTS O\n") +
				"where  DATA_OBJECT_ID is not null\n" +
				"  and  OBJECT_TYPE like 'TABLE%'\n" +
				"  and  TEMPORARY='N'\n" +
				(this.cdb ? "  and  CON_ID > 2\n" : "") +
				where,
				ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
		ResultSet rs = ps.executeQuery();
		boolean firstValue = true;
		int recordCount = 0;
		while (rs.next()) {
			if (firstValue) {
				firstValue = false;
			} else {
				sb.append(",");
			}
			sb.append(rs.getInt(1));
			recordCount++;
			if (recordCount > 999) {
				// ORA-01795
				sb.append(")");
				if (exclude) {
					sb.append(" and DATA_OBJ# not in (");
				} else {
					sb.append(" and DATA_OBJ# in (");
				}
				firstValue = true;
				recordCount = 0;
			}
		}
		sb.append(")");
		rs.close(); rs = null;
		ps.close(); ps = null;
		return sb.toString();
	}

	public String getConUidsList(final Connection connection) throws SQLException {
		if (cdb) {
			final StringBuilder sb = new StringBuilder(256);
			sb.append(" and SRC_CON_UID in (");
			// We do not need CDB$ROOT and PDB$SEED
			PreparedStatement statement = connection.prepareStatement(
					"select CON_UID from V$CONTAINERS where CON_ID > 2");
			ResultSet rs = statement.executeQuery();
			boolean first = true;
			while (rs.next()) {
				if (first) {
					first = false;
				} else {
					sb.append(",");
				}
				sb.append(rs.getLong(1));
			}
			sb.append("");
			if (first) {
				return "";
			} else {
				return sb.toString() + ")";
			}
		} else {
			return null;
		}
	}

	public String getVersionString() {
		return versionString;
	}

	public int getVersionMajor() {
		return versionMajor;
	}

	public short getInstanceNumber() {
		return instanceNumber;
	}

	public String getInstanceName() {
		return instanceName;
	}

	public String getHostName() {
		return hostName;
	}

	public int getCpuCoreCount() {
		return cpuCoreCount;
	}

	public long getDbId() {
		return dbId;
	}

	public String getDatabaseName() {
		return databaseName;
	}

	public String getPlatformName() {
		return platformName;
	}

	public boolean isCdb() {
		return cdb;
	}

	public boolean isCdbRoot() {
		return cdbRoot;
	}

	public Schema getSchema() {
		return schema;
	}

	public String getDbCharset() {
		return dbCharset;
	}

	public String getDbNCharCharset() {
		return dbNCharCharset;
	}

	public String getDbUniqueName() {
		return dbUniqueName;
	}

}
