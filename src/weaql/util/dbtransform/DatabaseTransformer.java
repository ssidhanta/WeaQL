package weaql.util.dbtransform;


import weaql.common.util.ConnectionFactory;
import weaql.common.util.ExitCode;
import org.apache.commons.dbutils.DbUtils;
import weaql.common.util.RuntimeUtils;
import weaql.common.util.defaults.DatabaseDefaults;
import weaql.common.util.defaults.ScratchpadDefaults;
import weaql.common.util.DatabaseProperties;

import java.sql.*;
import java.util.HashSet;
import java.util.Set;


/**
 * Created by dnlopes on 16/06/15.
 */
public class DatabaseTransformer
{

	private String databaseName;
	private DatabaseProperties dbProps;
	private Connection connection;
	private DatabaseMetaData metadata;

	public DatabaseTransformer(String databaseHost, String databaseName)
	{
		this.dbProps = new DatabaseProperties(DatabaseDefaults.DEFAULT_USER, DatabaseDefaults.DEFAULT_PASSWORD,
				databaseHost, DatabaseDefaults.DEFAULT_MYSQL_PORT);
		this.databaseName = databaseName;

		setup();
	}

	public void transformDatabase()
	{
		dropForeignKeys();
		addMetadataColumns();
		createAuxiliarProcedures();

		try
		{
			this.connection.commit();
			this.connection.close();
		} catch(SQLException e)
		{
			this.exitGracefully(e);
		}
	}

	private void setup()
	{
		Statement stat = null;
		try
		{
			this.connection = ConnectionFactory.getDefaultConnection(this.dbProps, this.databaseName);
			this.connection.setAutoCommit(false);
			this.metadata = this.connection.getMetaData();
			stat = this.connection.createStatement();

		} catch(SQLException e)
		{
			DbUtils.closeQuietly(stat);
			this.exitGracefully(e);
		}
	}

	private void dropForeignKeys()
	{
		try
		{
			ResultSet rs = metadata.getTables(null, null, "%", null);

			while(rs.next())
			{
				String tableName = rs.getString(3);

				if(tableName.startsWith(ScratchpadDefaults.SCRATCHPAD_TABLE_ALIAS_PREFIX))
					continue;

				dropForeignKeyFrom(tableName);
			}

		} catch(SQLException e)
		{
			exitGracefully(e);
		}

		System.out.println("foreign keys constraints droped");
	}

	private void dropForeignKeyFrom(String table)
	{
		Statement stat = null;
		Set<String> seenFks = new HashSet<>();

		try
		{
			ResultSet rs = this.metadata.getImportedKeys(null, null, table);
			while(rs.next())
			{
				String fkName = rs.getString(12);

				if(fkName == null)
					throw new SQLException(
							"please specify fk constraints by name in order to be automatically " + "dropped");

				if(seenFks.contains(fkName))
					continue;

				stat = this.connection.createStatement();
				String sql = "ALTER TABLE " + table + " DROP FOREIGN KEY " + fkName;
				stat.execute(sql);
				seenFks.add(fkName);
			}

		} catch(SQLException e)
		{
			DbUtils.closeQuietly(stat);
			this.exitGracefully(e);
		}
	}

	private void exitGracefully(SQLException e)
	{
		System.out.println("something went wrong and all changes will be rolledback");
		DbUtils.rollbackAndCloseQuietly(this.connection);
		RuntimeUtils.throwRunTimeException(e.getMessage(), ExitCode.INVALIDUSAGE);
	}

	private void addMetadataColumns()
	{
		try
		{
			ResultSet rs = metadata.getTables(null, null, "%", null);

			while(rs.next())
			{
				String tableName = rs.getString(3);

				if(tableName.startsWith(ScratchpadDefaults.SCRATCHPAD_TABLE_ALIAS_PREFIX))
					continue;

				addMetadataForTable(tableName);
			}

		} catch(SQLException e)
		{
			exitGracefully(e);
		}

		System.out.println("metadata columns added");
	}

	private void addMetadataForTable(String tableName)
	{
		ResultSet rs = null;
		Statement stat = null;

		try
		{
			rs = this.metadata.getColumns(null, null, tableName, null);
			stat = this.connection.createStatement();

			while(rs.next())
			{
				String columnName = rs.getString(4);

				if(columnName.compareTo(DatabaseDefaults.DELETED_COLUMN) == 0)
				{
					String sql = "ALTER TABLE " + tableName + " DROP COLUMN " + DatabaseDefaults.DELETED_COLUMN;
					stat.execute(sql);
				}
				if(columnName.compareTo(DatabaseDefaults.DELETED_CLOCK_COLUMN) == 0)
				{
					String sql = "ALTER TABLE " + tableName + " DROP COLUMN " + DatabaseDefaults.DELETED_CLOCK_COLUMN;
					stat.execute(sql);
				}
				if(columnName.compareTo(DatabaseDefaults.CONTENT_CLOCK_COLUMN) == 0)
				{
					String sql = "ALTER TABLE " + tableName + " DROP COLUMN " + DatabaseDefaults.CONTENT_CLOCK_COLUMN;
					stat.execute(sql);
				}
			}

		} catch(SQLException e)
		{
			DbUtils.closeQuietly(rs);
			DbUtils.closeQuietly(stat);
			this.exitGracefully(e);
		}

		try
		{
			String sql = "ALTER TABLE " + tableName + " ADD " + DatabaseDefaults.DELETED_COLUMN + " boolean default 0";
			stat.execute(sql);
			sql = "ALTER TABLE " + tableName + " ADD " + DatabaseDefaults.DELETED_CLOCK_COLUMN + " varchar(100)";
			stat.execute(sql);
			sql = "ALTER TABLE " + tableName + " ADD " + DatabaseDefaults.CONTENT_CLOCK_COLUMN + " varchar(100)";
			stat.execute(sql);

		} catch(SQLException e)
		{
			DbUtils.closeQuietly(rs);
			DbUtils.closeQuietly(stat);
			this.exitGracefully(e);
		}
	}

	private void createAuxiliarProcedures()
	{
		Statement stat = null;
		try
		{
			stat = this.connection.createStatement();
			stat.execute("use " + this.databaseName);

			stat.execute(Procedures.DROP_MAX_CLOCK);
			stat.execute(Procedures.MAX_CLOCK);

			stat.execute(Procedures.DROP_IS_STRICTLY_GREATER);
			stat.execute(Procedures.IS_STRICTLY_GREATER);

			stat.execute(Procedures.DROP_IS_CONCURRENT_OR_GREATER);
			stat.execute(Procedures.IS_CONCURRENT_OR_GREATER);

			stat.execute(Procedures.DROP_CLOCK_IS_GREATER);
			stat.execute(Procedures.CLOCK_IS_GREATER);

			stat.execute(Procedures.DROP_COMPARE_CLOCKS);
			//stat.execute(Procedures.COMPARE_CLOCKS);
			stat.execute(Procedures.DROP_ARE_CONCURRENT_CLOCKS);
			//stat.execute(Procedures.ARE_CONCURRENT_CLOCKS);

		} catch(SQLException e)
		{
			DbUtils.closeQuietly(stat);
			this.exitGracefully(e);
		}

		System.out.println("auxiliar procedures created");
	}

	private interface Procedures
	{

		String DROP_COMPARE_CLOCKS = "DROP FUNCTION IF EXISTS compareClocks";
		String COMPARE_CLOCKS = "CREATE FUNCTION compareClocks" +
				"(currentClock CHAR(100), newClock CHAR(100)) RETURNS int DETERMINISTIC BEGIN DECLARE isConcurrent " +
				"BOOL; DECLARE isLesser BOOL; DECLARE dumbFlag BOOL; DECLARE isGreater BOOL; DECLARE cycleCond BOOL;" +
				" " +
				"DECLARE returnValue INT; SET @dumbFlag = FALSE; SET @returnValue = 0; SET @isConcurrent = FALSE; " +
				"SET" +
				" " +
				"@isLesser = FALSE; SET @isGreater = FALSE; IF(currentClock IS NULL) then RETURN 1; END IF; loopTag:" +
				" " +
				"WHILE (TRUE) DO SET @currEntry = CONVERT ( LEFT(currentClock, 1), SIGNED); SET @newEntry = CONVERT " +
				"(" +
				" " +
				"LEFT(newClock, 1), SIGNED); IF(@currEntry > @newEntry) then SET @dumbFlag = TRUE; IF(@isLesser) " +
				"then" +
				" " +
				"SET @isConcurrent = TRUE; LEAVE loopTag; END IF; SET @isGreater = TRUE; ELSEIF(@currEntry < " +
				"@newEntry) then IF(@isGreater) then SET @isConcurrent = TRUE; IF(@dumbFlag = FALSE) then SET " +
				"@isGreater = TRUE; END IF; LEAVE loopTag; END IF; SET @isLesser = TRUE; END IF; IF (LENGTH" +
				"(currentClock) = 1) then LEAVE loopTag; END IF; SET currentClock = SUBSTRING(currentClock, LOCATE" +
				"('-', currentClock) + 1); SET newClock = SUBSTRING(newClock, LOCATE('-', newClock) + 1); END WHILE;" +
				" " +
				"IF(@isConcurrent AND @dumbFlag = FALSE) then SELECT 0 INTO @returnValue; ELSEIF(@isLesser) then " +
				"SELECT 1 INTO @returnValue; ELSE SELECT -1 INTO @returnValue; END IF; RETURN @returnValue; END";

		String DROP_ARE_CONCURRENT_CLOCKS = "DROP FUNCTION IF EXISTS areConcurrentClocks";
		String ARE_CONCURRENT_CLOCKS = "CREATE FUNCTION areConcurrentClocks(currentClock CHAR(100), " +
				"newClock CHAR(100)) RETURNS BOOL DETERMINISTIC BEGIN DECLARE isConcurrent BOOL; DECLARE isLesser " +
				"BOOL; DECLARE isGreater BOOL; DECLARE cycleCond BOOL; DECLARE returnValue BOOL; SET @returnValue = " +
				"FALSE; SET @isConcurrent = FALSE; SET @isLesser = FALSE; SET @isGreater = FALSE; IF(currentClock IS" +
				" " +
				"NULL) then RETURN 0; END IF; loopTag: WHILE (TRUE) DO SET @index = LOCATE('-', currentClock); IF" +
				"(@index = 0) then SET @currEntry = CONVERT (currentClock, SIGNED); SET @newEntry = CONVERT " +
				"(newClock," +
				" SIGNED); ELSE SET @index = LOCATE('-', currentClock); SET @index2 = LOCATE('-', newClock); SET " +
				"@currEntry = CONVERT (LEFT(currentClock, @index-1), SIGNED); SET @newEntry = CONVERT (LEFT" +
				"(newClock," +
				" " +
				"@index2-1), SIGNED); END IF; IF(@currEntry > @newEntry) then IF(@isLesser) then SET @isConcurrent =" +
				" " +
				"TRUE; LEAVE loopTag; END IF; SET @isGreater = TRUE; ELSEIF(@currEntry < @newEntry) then IF" +
				"(@isGreater) then SET @isConcurrent = TRUE; LEAVE loopTag; END IF; SET @isLesser = TRUE; END IF; IF" +
				" " +
				"(LOCATE('-', currentClock) = 0) then LEAVE loopTag; END IF; SET currentClock = SUBSTRING" +
				"(currentClock, LOCATE('-', currentClock) + 1); SET newClock = SUBSTRING(newClock, LOCATE('-', " +
				"newClock) + 1); END WHILE; IF(@isConcurrent) then SELECT TRUE INTO @returnValue; ELSE SELECT FALSE " +
				"INTO @returnValue; END IF; RETURN @returnValue; END";

		String DROP_CLOCK_IS_GREATER = "DROP FUNCTION IF EXISTS clockIsGreater";
		String CLOCK_IS_GREATER = "CREATE FUNCTION clockIsGreater(currentClock CHAR(100), newClock CHAR" +
				"(100)) RETURNS BOOL DETERMINISTIC BEGIN DECLARE isConcurrent BOOL; DECLARE isLesser BOOL; DECLARE " +
				"dumbFlag BOOL; DECLARE isGreater BOOL; DECLARE cycleCond BOOL; DECLARE returnValue BOOL; SET " +
				"@dumbFlag = FALSE; SET @returnValue = FALSE; SET @isConcurrent = FALSE; SET @isLesser = FALSE; SET " +
				"@isGreater = FALSE; IF(currentClock IS NULL) then RETURN TRUE; END IF; loopTag: WHILE (TRUE) DO SET" +
				" " +
				"@index = LOCATE('-', currentClock); IF(@index = 0) then SET @currEntry = CONVERT (currentClock, " +
				"SIGNED); SET @newEntry = CONVERT (newClock, SIGNED); ELSE SET @index = LOCATE('-', currentClock); " +
				"SET" +
				" @index2 = LOCATE('-', newClock); SET @currEntry = CONVERT (LEFT(currentClock, @index-1), SIGNED); " +
				"SET @newEntry = CONVERT (LEFT(newClock, @index2-1), SIGNED); END IF; IF(@currEntry > @newEntry) " +
				"then" +
				" " +
				"SET @dumbFlag = TRUE; IF(@isLesser) then SET @isConcurrent = TRUE; LEAVE loopTag; END IF; SET " +
				"@isGreater = TRUE; ELSEIF(@currEntry < @newEntry) then IF(@isGreater) then SET @isConcurrent = " +
				"TRUE;" +
				" " +
				"IF(@dumbFlag = FALSE) then SET @isGreater = TRUE; END IF; LEAVE loopTag; END IF; SET @isLesser = " +
				"TRUE; END IF; IF (LOCATE('-', currentClock) = 0) then LEAVE loopTag; END IF; SET currentClock = " +
				"SUBSTRING(currentClock, LOCATE('-', currentClock) + 1); SET newClock = SUBSTRING(newClock, LOCATE" +
				"('-', newClock) + 1); END WHILE; IF(@isConcurrent AND @dumbFlag = FALSE) then SELECT TRUE INTO " +
				"@returnValue; ELSEIF(@isLesser) then SELECT TRUE INTO @returnValue; ELSE SELECT FALSE INTO " +
				"@returnValue; END IF; RETURN @returnValue; END";

		String DROP_IS_CONCURRENT_OR_GREATER = "DROP FUNCTION IF EXISTS isConcurrentOrGreaterClock";
		String IS_CONCURRENT_OR_GREATER = "CREATE FUNCTION isConcurrentOrGreaterClock(currentClock CHAR" +
				"(100), " +
				"newClock CHAR(100)) RETURNS BOOL DETERMINISTIC BEGIN DECLARE isConcurrent BOOL; DECLARE isLesser " +
				"BOOL; DECLARE dumbFlag BOOL; DECLARE isGreater BOOL; DECLARE cycleCond BOOL; DECLARE returnValue " +
				"BOOL; SET @dumbFlag = FALSE; SET @returnValue = FALSE; SET @isConcurrent = FALSE; SET @isLesser = " +
				"FALSE; SET @isGreater = FALSE; IF(currentClock IS NULL) then RETURN 1; END IF; loopTag: WHILE " +
				"(TRUE)" +
				" " +
				"DO SET @index = LOCATE('-', currentClock); IF(@index = 0) then SET @currEntry = CONVERT " +
				"(currentClock, SIGNED); SET @newEntry = CONVERT (newClock, SIGNED); ELSE SET @index = LOCATE('-', " +
				"currentClock); SET @index2 = LOCATE('-', newClock); SET @currEntry = CONVERT (LEFT(currentClock, " +
				"@index-1), SIGNED); SET @newEntry = CONVERT (LEFT(newClock, @index2-1), SIGNED); END IF; IF" +
				"(@currEntry > @newEntry) then SET @dumbFlag = TRUE; IF(@isLesser) then SET @isConcurrent = TRUE; " +
				"LEAVE loopTag; END IF; SET @isGreater = TRUE; ELSEIF(@currEntry < @newEntry) then IF(@isGreater) " +
				"then" +
				" SET @isConcurrent = TRUE; IF(@dumbFlag = FALSE) then SET @isGreater = TRUE; END IF; LEAVE loopTag;" +
				" " +
				"END IF; SET @isLesser = TRUE; END IF; IF (LOCATE('-', currentClock) = 0) then LEAVE loopTag; END " +
				"IF;" +
				" " +
				"SET currentClock = SUBSTRING(currentClock, LOCATE('-', currentClock) + 1); SET newClock = SUBSTRING" +
				"(newClock, LOCATE('-', newClock) + 1); END WHILE; IF(@isConcurrent) then SELECT TRUE INTO " +
				"@returnValue; ELSEIF(@isLesser) then SELECT TRUE INTO @returnValue; ELSE SELECT FALSE INTO " +
				"@returnValue; END IF; RETURN @returnValue; END";

		String DROP_IS_STRICTLY_GREATER = "DROP FUNCTION IF EXISTS isStrictlyGreater";
		String IS_STRICTLY_GREATER = "CREATE FUNCTION isStrictlyGreater(currentClock CHAR(100), " +
				"newClock" +
				" " +
				"CHAR(100)) RETURNS BOOL DETERMINISTIC BEGIN DECLARE isConcurrent BOOL; DECLARE isLesser BOOL; " +
				"DECLARE" +
				" dumbFlag BOOL; DECLARE isGreater BOOL; DECLARE cycleCond BOOL; DECLARE returnValue BOOL; SET " +
				"@dumbFlag = FALSE; SET @returnValue = FALSE; SET @isConcurrent = FALSE; SET @isLesser = FALSE; SET " +
				"@isGreater = FALSE; IF(currentClock IS NULL) then RETURN TRUE; END IF; loopTag: WHILE (TRUE) DO SET" +
				" " +
				"@index = LOCATE('-', currentClock); IF(@index = 0) then SET @currEntry = CONVERT (currentClock, " +
				"SIGNED); SET @newEntry = CONVERT (newClock, SIGNED); ELSE SET @index = LOCATE('-', currentClock); " +
				"SET" +
				" @index2 = LOCATE('-', newClock); SET @currEntry = CONVERT (LEFT(currentClock, @index-1), SIGNED); " +
				"SET @newEntry = CONVERT (LEFT(newClock, @index2-1), SIGNED); END IF; IF(@currEntry > @newEntry) " +
				"then" +
				" " +
				"SET @dumbFlag = TRUE; IF(@isLesser) then SET @isConcurrent = TRUE; LEAVE loopTag; END IF; SET " +
				"@isGreater = TRUE; ELSEIF(@currEntry < @newEntry) then IF(@isGreater) then SET @isConcurrent = " +
				"TRUE;" +
				" " +
				"IF(@dumbFlag = FALSE) then SET @isGreater = TRUE; END IF; LEAVE loopTag; END IF; SET @isLesser = " +
				"TRUE; END IF; IF (LOCATE('-', currentClock) = 0) then LEAVE loopTag; END IF; SET currentClock = " +
				"SUBSTRING(currentClock, LOCATE('-', currentClock) + 1); SET newClock = SUBSTRING(newClock, LOCATE" +
				"('-', newClock) + 1); END WHILE; IF(@isConcurrent) then SELECT FALSE INTO @returnValue; ELSEIF" +
				"(@isLesser) then SELECT TRUE INTO @returnValue; ELSE SELECT FALSE INTO @returnValue; END IF; RETURN" +
				" " +
				"@returnValue; END";

		String DROP_MAX_CLOCK = "DROP FUNCTION IF EXISTS maxClock";
		String MAX_CLOCK = "CREATE FUNCTION maxClock(currentClock CHAR(100), newClock CHAR(100)) " +
				"RETURNS" +
				" " +
				"CHAR(200) DETERMINISTIC BEGIN DECLARE returnValue CHAR(200); SET @returnValue = ''; IF(currentClock" +
				" " +
				"IS NULL) then RETURN 'NULL'; END IF; loopTag: WHILE (TRUE) DO SET @index = LOCATE('-', " +
				"currentClock)" +
				";" +
				" IF(@index = 0) then SET @currEntry = CONVERT (currentClock, SIGNED); SET @newEntry = CONVERT " +
				"(newClock, SIGNED); ELSE SET @index = LOCATE('-', currentClock); SET @index2 = LOCATE('-', " +
				"newClock)" +
				";" +
				" SET @currEntry = CONVERT (LEFT(currentClock, @index-1), SIGNED); SET @newEntry = CONVERT (LEFT" +
				"(newClock, @index2-1), SIGNED); END IF; IF(@currEntry >= @newEntry) then SET @returnValue = CONCAT" +
				"(@returnValue, @currEntry); ELSE SET @returnValue = CONCAT(@returnValue, @newEntry); END IF; IF " +
				"(LOCATE('-', currentClock) = 0) then LEAVE loopTag; END IF; SET @returnValue = CONCAT(@returnValue," +
				" " +
				"'-'); SET currentClock = SUBSTRING(currentClock, LOCATE('-', currentClock) + 1); SET newClock = " +
				"SUBSTRING(newClock, LOCATE('-', newClock) + 1); END WHILE; RETURN @returnValue; END";
	}
}
