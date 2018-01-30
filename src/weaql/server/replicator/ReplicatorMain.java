package weaql.server.replicator;


import weaql.common.nodes.NodeConfig;
import weaql.common.util.WeaQLEnvironment;
import weaql.common.util.ExitCode;
import weaql.common.util.Topology;
import weaql.common.util.exception.ConfigurationLoadException;
import weaql.common.util.exception.InitComponentFailureException;
import weaql.common.util.exception.InvalidConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Created by dnlopes on 23/03/15.
 */
public class ReplicatorMain
{

	private static final Logger LOG = LoggerFactory.getLogger(ReplicatorMain.class);

	public static void main(String args[])
			throws ConfigurationLoadException, InitComponentFailureException, InvalidConfigurationException
	{
		if(args.length != 3)
		{
			System.err.print("usage: java -jar <jarfile> <topologyFile> <environmentFile> " +
					"<id>");
			System.exit(ExitCode.WRONG_ARGUMENTS_NUMBER);
		}

		String topologyFile = args[0];
		String environmentFile = args[1];
		int id = Integer.parseInt(args[2]);

		Topology.setupTopology(topologyFile);
		WeaQLEnvironment.setupEnvironment(environmentFile);
		WeaQLEnvironment.printEnvironment();

		NodeConfig config = Topology.getInstance().getReplicatorConfigWithIndex(id);

		LOG.info("starting replicator {}", config.getId());
		Replicator replicator = new Replicator(config);
	}

}
