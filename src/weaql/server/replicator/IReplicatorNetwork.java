package weaql.server.replicator;

import weaql.common.thrift.CRDTCompiledTransaction;
import weaql.common.thrift.CoordinatorRequest;
import weaql.common.thrift.CoordinatorResponse;
import weaql.common.util.exception.SocketConnectionException;

import java.util.List;


/**
 * @author dnlopes
 *         This interface defines methods for all Replicator communications.
 *         All methods should be thread-safe, because this class is used by multiple threads to send events to
 *         remote nodes
 */
public interface IReplicatorNetwork
{
	void send(CRDTCompiledTransaction transaction);
	void send(List<CRDTCompiledTransaction> transactions);
	void openConnections();

	CoordinatorResponse sendRequestToCoordinator(CoordinatorRequest req) throws SocketConnectionException;
}
