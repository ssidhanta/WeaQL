package weaql.server.execution;


import weaql.common.nodes.NodeConfig;
import weaql.common.thrift.CRDTCompiledTransaction;
import weaql.common.thrift.Status;
import weaql.common.util.ConnectionFactory;
import org.apache.commons.dbutils.DbUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import weaql.server.replicator.Replicator;
import weaql.server.util.TransactionCommitFailureException;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;


/**
 * Created by dnlopes on 06/04/15.
 */
public class DBCommitterAgent implements DBCommitter
{

	private static final Logger LOG = LoggerFactory.getLogger(DBCommitterAgent.class);
	private static final Status SUCCESS_STATUS = new Status(true, null);

	private Connection connection;
	private String lastError;

	public DBCommitterAgent(NodeConfig config) throws SQLException
	{
		this.connection = ConnectionFactory.getDefaultConnection(config);
	}

	@Override
	public Status commitTrx(CRDTCompiledTransaction txn) throws TransactionCommitFailureException
	{
		int tries = 7;

		while(true)
		{
			tries++;
			if(tries % LOG_ERROR_FREQUENCY == 0)
				LOG.warn("already tried {} to commit without success ({})", tries, lastError);

			boolean commitDecision = tryCommit(txn);

			if(commitDecision)
				return SUCCESS_STATUS;
			else
				Replicator.abortCounter.incrementAndGet();
		}
	}

	private boolean tryCommit(CRDTCompiledTransaction op) throws TransactionCommitFailureException
	{
		Statement stat = null;
		boolean success = false;

		try
		{
			stat = this.connection.createStatement();

			for(String sqlOp : op.getOps())
				stat.addBatch(sqlOp);

			stat.executeBatch();
			connection.commit();
			success = true;

			LOG.trace("txn ({}) committed", op.getTxnClock());

		} catch(SQLException e)
		{
			try
			{
				lastError = e.getMessage();
				DbUtils.rollback(this.connection);
			} catch(SQLException e1)
			{
				LOG.warn("failed to rollback txn ({})", op.getTxnClock(), e1);
			}
		} finally
		{
			DbUtils.closeQuietly(stat);
		}

		return success;
	}
}
