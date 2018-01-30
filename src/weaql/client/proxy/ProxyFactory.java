package weaql.client.proxy;


import weaql.client.proxy.hook.TransactionsLogWritter;
import weaql.common.nodes.NodeConfig;
import weaql.common.util.Topology;

import java.sql.SQLException;


/**
 * Created by dnlopes on 02/09/15.
 */
public class ProxyFactory
{

	private static final ProxyFactory ourInstance = new ProxyFactory();
	private static TransactionsLogWritter hook;
	private static NodeConfig PROXY_CONFIG;
	private static int proxiesCounter;

	public static ProxyFactory getInstance()
	{
		return ourInstance;
	}

	private ProxyFactory()
	{
		PROXY_CONFIG = Topology.getInstance().getProxyConfigWithIndex(Integer.parseInt(System.getProperty("proxyid")));
		proxiesCounter = 0;
		hook = new TransactionsLogWritter();
		Runtime.getRuntime().addShutdownHook(hook);
	}

	public static Proxy getProxyInstance() throws SQLException
	{
		Proxy proxy = createWSSandboxProxy();
		hook.addProxy(proxy);
		//return createWriteThroughProxy();
		return proxy;
	}


	private static Proxy createWSSandboxProxy() throws SQLException
	{
		return new SandboxExecutionProxy(PROXY_CONFIG, assignProxyId());
	}

	private static synchronized int assignProxyId()
	{
		return proxiesCounter++;
	}
}
