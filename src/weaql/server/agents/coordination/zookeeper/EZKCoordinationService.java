package weaql.server.agents.coordination.zookeeper;


import org.apache.zookeeper.KeeperException;
import weaql.common.thrift.CoordinatorRequest;
import weaql.common.thrift.CoordinatorResponse;

/**
 * Created by dnlopes on 13/07/15.
 */
public interface EZKCoordinationService
{

	void init(String codeBasePath) throws KeeperException, InterruptedException;
	void closeExtension() throws InterruptedException;

	void cleanupDatabase() throws KeeperException, InterruptedException;
	CoordinatorResponse sendRequest(CoordinatorRequest request);

}
