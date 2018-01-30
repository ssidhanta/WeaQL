package weaql.server.replicator;


import weaql.common.util.exception.InitComponentFailureException;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import weaql.common.thrift.ReplicatorRPC;


/**
 * Created by dnlopes on 20/03/15.
 */
public class ReceiverThread implements Runnable
{

	static final Logger LOG = LoggerFactory.getLogger(ReceiverThread.class);

	private Replicator me;
	private TServer server;

	public ReceiverThread(Replicator node) throws TTransportException, InitComponentFailureException
	{
		this.me = node;
		RPCHandler handler = new RPCHandler(this.me);
		ReplicatorRPC.Processor processor = new ReplicatorRPC.Processor(handler);
		TServerTransport serverTransport = new TServerSocket(node.getSocketAddress().getPort());
		this.server = new TThreadPoolServer(new TThreadPoolServer.Args(serverTransport).processor(processor));
	}

	@Override
	public void run()
	{
		LOG.info("starting replicator server thread on port {}", this.me.getSocketAddress().getPort());
		this.server.serve();
	}
}
