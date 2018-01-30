package weaql.server.agents.dispatcher;


import weaql.common.thrift.CRDTCompiledTransaction;
import weaql.server.util.TransactionCommitFailureException;


/**
 * Created by dnlopes on 08/10/15.
 * <p/>
 * A dispatcher agent is responsible for propagating transactions to remote replicators when
 * appropriate.
 * When the local replicator sucessfully commits a transaction, it dispatch the transaction to this agent.
 * This agent is responsible for sending the transaction to remote replicators following a specific policy
 * Currently there are two dispatch policies implemented: basic dispatcher and aggregator dispatcher
 * <p/>
 *
 */
public interface DispatcherAgent
{
	void dispatchTransaction(CRDTCompiledTransaction op) throws TransactionCommitFailureException;
}
