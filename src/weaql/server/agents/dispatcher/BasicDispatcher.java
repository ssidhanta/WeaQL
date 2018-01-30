package weaql.server.agents.dispatcher;


import weaql.common.thrift.CRDTCompiledTransaction;
import weaql.server.replicator.IReplicatorNetwork;
import weaql.server.replicator.Replicator;


/**
 * Created by dnlopes on 08/10/15.
 * Basic dispatcher immediately forwards incoming transactions to remote replicators.
 */
public class BasicDispatcher implements DispatcherAgent
{

	private final IReplicatorNetwork networkInterface;

	public BasicDispatcher(Replicator replicator)
	{
		this.networkInterface = replicator.getNetworkInterface();
	}

	@Override
	public void dispatchTransaction(CRDTCompiledTransaction transaction)
	{
		networkInterface.send(transaction);
	}
}
