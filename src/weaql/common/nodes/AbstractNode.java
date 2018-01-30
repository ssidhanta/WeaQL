package weaql.common.nodes;



import java.net.InetSocketAddress;


/**
 * Created by dnlopes on 15/03/15.
 */
public abstract class AbstractNode
{

	private InetSocketAddress socketAddress;
	protected NodeConfig config;

	public AbstractNode(NodeConfig config)
	{
		this.socketAddress = new InetSocketAddress(config.getHost(), config.getPort());
		this.config = config;
	}

	public InetSocketAddress getSocketAddress()
	{
		return this.socketAddress;
	}

	public NodeConfig getConfig()
	{
		return this.config;
	}
}


