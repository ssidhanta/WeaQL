package weaql.server.agents.deliver;


import weaql.common.thrift.CRDTCompiledTransaction;
import weaql.server.replicator.Replicator;
import weaql.server.util.TransactionCommitFailureException;


/**
 * Created by dnlopes on 24/05/15.
 */
public class NoOrderDeliverAgent implements DeliverAgent
{

	private final Replicator replicator;

	public NoOrderDeliverAgent(Replicator replicator)
	{
		this.replicator = replicator;
	}

	@Override
	public void deliverTransaction(CRDTCompiledTransaction op) throws TransactionCommitFailureException
	{
		this.replicator.commitOperation(op, true);
	}
}
