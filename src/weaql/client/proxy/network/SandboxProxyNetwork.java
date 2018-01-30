package weaql.client.proxy.network;


import weaql.common.nodes.AbstractNetwork;
import weaql.common.nodes.NodeConfig;
import weaql.common.thrift.CRDTPreCompiledTransaction;
import weaql.common.thrift.Status;
import weaql.common.util.exception.SocketConnectionException;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import weaql.common.thrift.ReplicatorRPC;


/**
 * Created by dnlopes on 02/09/15.
 */
public class SandboxProxyNetwork extends AbstractNetwork implements IProxyNetwork
{

	private final NodeConfig replicatorConfig;
	private ReplicatorRPC.Client replicatorRpc;

	public SandboxProxyNetwork(NodeConfig proxyConfig)
	{
		super(proxyConfig);

		if(LOG.isTraceEnabled())
			LOG.trace("setting up rpc connection to replicator");

		this.replicatorConfig = proxyConfig.getReplicatorConfig();
		this.replicatorRpc = createReplicatorConnection();
	}

	@Override
	public Status commitOperation(CRDTPreCompiledTransaction crdtTransaction) throws SocketConnectionException
	{
		if(replicatorRpc == null)
			replicatorRpc = createReplicatorConnection();

		if(replicatorRpc == null)
			throw new SocketConnectionException("cannot open connection to replicator");
		try
		{
			return replicatorRpc.commitOperation(crdtTransaction);
		} catch(TException e)
		{
			LOG.warn("communication problem between proxy and replicator: {}", e.getMessage(), e);
			return new Status(false, e.getMessage());
		}
	}

	private ReplicatorRPC.Client createReplicatorConnection()
	{
		TTransport newTransport = new TSocket(this.replicatorConfig.getHost(), this.replicatorConfig.getPort());
		try
		{
			newTransport.open();
		} catch(TTransportException e)
		{
			LOG.warn("failed to open connection to replicator node");
			return null;
		}

		TProtocol protocol = new TBinaryProtocol.Factory().getProtocol(newTransport);
		return new ReplicatorRPC.Client(protocol);
	}
}
