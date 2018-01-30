package weaql.server.execution;


import weaql.common.thrift.CRDTCompiledTransaction;
import weaql.common.thrift.Status;
import weaql.server.util.TransactionCommitFailureException;


/**
 * Created by dnlopes on 06/04/15.
 * Interface to execute transactions on main storage
 */
public interface DBCommitter
{

	int MAX_RETRIES = 50;
	int LOG_ERROR_FREQUENCY = 100;

	/**
	 * Attemps to commit a transaction in main storage
	 * @param op
	 * @return true if commit succeeds
	 */
	Status commitTrx(CRDTCompiledTransaction txn) throws TransactionCommitFailureException;
}
