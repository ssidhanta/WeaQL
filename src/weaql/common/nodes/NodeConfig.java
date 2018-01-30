package weaql.common.nodes;


import weaql.common.util.RuntimeUtils;
import weaql.common.util.DatabaseProperties;
import weaql.common.util.ExitCode;


/**
 * Created by dnlopes on 22/03/15.
 */
public class NodeConfig
{

	private final int id;
	private final String host;
	private final int port;
	private final Role role;

	private final DatabaseProperties dbProps;
	private NodeConfig replicatorConfig;

	// use with proxy node
	public NodeConfig(Role role, int id, String host, int port, DatabaseProperties props, NodeConfig replicatorConfig)
	{
		this.role = role;
		this.id = id;
		this.host = host;
		this.port = port;
		this.dbProps = props;
		this.replicatorConfig = replicatorConfig;

		if(this.dbProps == null && this.role != Role.COORDINATOR)
			RuntimeUtils.throwRunTimeException("dbProps not defined", ExitCode.NOINITIALIZATION);
	}

	public NodeConfig(Role role, int id, String host, int port, DatabaseProperties props)
	{
		this.role = role;
		this.id = id;
		this.host = host;
		this.port = port;
		this.dbProps = props;
		this.replicatorConfig = null;

		if(this.dbProps == null && this.role != Role.COORDINATOR)
			RuntimeUtils.throwRunTimeException("dbProps not defined", ExitCode.NOINITIALIZATION);
	}

	public NodeConfig getReplicatorConfig()
	{
		if(!(this.role == Role.PROXY))
			RuntimeUtils.throwRunTimeException("this config obj does not belong to a proxy", ExitCode.INVALIDUSAGE);

		return this.replicatorConfig;
	}

	public int getId()
	{
		return id;
	}

	public String getHost()
	{
		return host;
	}

	public int getPort()
	{
		return port;
	}

	public String getName()
	{
		return this.role + "-" + this.id;
	}

	public DatabaseProperties getDbProps()
	{
		return this.dbProps;
	}
}
