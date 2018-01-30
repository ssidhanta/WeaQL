package weaql.client.execution.temporary.scratchpad;


import weaql.client.execution.TransactionContext;
import weaql.client.operation.SQLOperationType;
import weaql.client.operation.SQLSelect;
import weaql.client.operation.SQLWriteOperation;
import weaql.client.execution.temporary.WriteSet;
import weaql.client.execution.temporary.scratchpad.agent.IExecutorAgent;
import weaql.client.execution.temporary.scratchpad.agent.ExecutorAgent;
import weaql.common.database.Record;
import weaql.common.database.SQLInterface;
import weaql.common.util.defaults.ScratchpadDefaults;
import org.apache.commons.dbutils.DbUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;


/**
 * Created by dnlopes on 25/09/15.
 */
public class DBScratchpad implements IDBScratchpad
{

	private static final Logger LOG = LoggerFactory.getLogger(DBScratchpad.class);

	private int scratchpadId;
	private Map<String, IExecutorAgent> executers;
	private SQLInterface sqlInterface;
	private TransactionContext txnRecord;
	private WriteSet writeSet;

	public DBScratchpad(SQLInterface sqlInterface, TransactionContext txnRecord) throws SQLException
	{
		this.executers = new HashMap<>();
		this.sqlInterface = sqlInterface;
		this.txnRecord = txnRecord;
		this.writeSet = new WriteSet();

		assignScratchpadId();
		createDBExecuters();
	}

	@Override
	public ResultSet executeQuery(SQLSelect sqlSelect) throws SQLException
	{
                IExecutorAgent executor = null;
                if(this.executers!=null && sqlSelect!=null && sqlSelect.getTable()!=null && sqlSelect.getTable().getName()!=null)
                     executor = this.executers.get(sqlSelect.getTable().getName().toUpperCase());

		if(executor == null)
		{
                        if(sqlSelect!=null && sqlSelect.getTable()!=null && sqlSelect.getTable().getName()!=null)
                            throw new SQLException("executor agent not found for table " + sqlSelect.getDbTable().getName());
                        else
                                return null;
		} else
			return executor.executeTemporaryQuery(sqlSelect);
	}

	@Override
	public ResultSet executeQuery(String query) throws SQLException
	{
		return this.sqlInterface.executeQuery(query);
	}

	@Override
	public int executeUpdate(SQLWriteOperation sqlWriteOp) throws SQLException
	{
		if(sqlWriteOp.getOpType() == SQLOperationType.SELECT)
			throw new SQLException("update operation expected but instead we got a select query");
                IExecutorAgent agent = null;
                if(this.executers!=null && sqlWriteOp.getDbTable()!=null && sqlWriteOp.getDbTable().getName()!=null && this.executers.get(sqlWriteOp.getDbTable().getName().toUpperCase())!=null)    
                    agent = this.executers.get(sqlWriteOp.getDbTable().getName().toUpperCase());

		if(agent == null)
		{
                    if(sqlWriteOp.getDbTable()!=null && sqlWriteOp.getDbTable().getName()!=null)
                        throw new SQLException("executor agent not found for table " + sqlWriteOp.getDbTable().getName());
                    else
                        return 0;
		} else
		{
			return agent.executeTemporaryUpdate(sqlWriteOp);
		}
	}

	@Override
	public void clearScratchpad() throws SQLException
	{
		for(IExecutorAgent agent : this.executers.values())
			agent.clearExecutor();

		writeSet.clear();
	}

	@Override
	public WriteSet getWriteSet()
	{
		return writeSet;
	}

	@Override
	public List<Record> getScratchpadSnapshot() throws SQLException
	{
		List<Record> records = new LinkedList<>();

		for(IExecutorAgent executor : executers.values())
			executor.scanTemporaryTables(records);

		return records;
	}

	private void createDBExecuters() throws SQLException
	{
		DatabaseMetaData metadata = sqlInterface.getConnection().getMetaData();
		String[] types = {"TABLE"};
		ResultSet tblSet = metadata.getTables(null, null, "%", types);

		ArrayList<String> tempTables = new ArrayList<>();
		while(tblSet.next())
		{
			String tableName = tblSet.getString(3);
			if(tableName.startsWith(ScratchpadDefaults.SCRATCHPAD_TABLE_ALIAS_PREFIX))
				continue;

			tempTables.add(tableName);
		}

		Collections.sort(tempTables);

		for(int i = 0; i < tempTables.size(); i++)
		{
			String tableName = tempTables.get(i);
			IExecutorAgent executor = new ExecutorAgent(scratchpadId, i, tableName, this.sqlInterface, this,
					txnRecord);
			executor.setup(metadata, scratchpadId);
			this.sqlInterface.commit();
			this.executers.put(tableName.toUpperCase(), executor);
		}
	}

	private void assignScratchpadId()
	{
		createScratchpadIdsTable();

		for(; ; )
		{
			ResultSet rs = null;
			try
			{
				rs = sqlInterface.executeQuery("SELECT id FROM " + ScratchpadDefaults.SCRATCHPAD_IDS_TABLE + " WHERE" +
						" k = 1");

				rs.next();
				int id = rs.getInt(1);
				sqlInterface.executeUpdate(
						"UPDATE " + ScratchpadDefaults.SCRATCHPAD_IDS_TABLE + " SET id = id + 1 WHERE" +
								" " +
								"k = 1;");
				sqlInterface.commit();
				scratchpadId = id;
				return;
			} catch(SQLException e)
			{
				LOG.trace(e.getMessage());
			} finally
			{
				DbUtils.closeQuietly(rs);
			}
		}
	}

	private void createScratchpadIdsTable()
	{
		try
		{
			this.sqlInterface.executeUpdate("CREATE TABLE " + ScratchpadDefaults.SCRATCHPAD_IDS_TABLE + " ( k int " +
					"NOT" +
					" " +
					"NULL primary key, id " +
					"int)");
			this.sqlInterface.commit();
			this.sqlInterface.executeUpdate("INSERT INTO " + ScratchpadDefaults.SCRATCHPAD_IDS_TABLE + " VALUES (1," +
					"1)");
			this.sqlInterface.commit();

		} catch(SQLException e)
		{
			LOG.trace("scratchpad_id table already exists");
		}

	}

}
