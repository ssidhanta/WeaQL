package weaql.common.nodes;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Created by dnlopes on 21/03/15.
 */
public abstract class AbstractNetwork
{
	protected static final Logger LOG = LoggerFactory.getLogger(AbstractNetwork.class);
	protected NodeConfig me;

	public AbstractNetwork(NodeConfig node)
	{
		this.me = node;
	}
}
