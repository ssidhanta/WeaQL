package weaql.client.proxy.network;


import weaql.common.thrift.CRDTPreCompiledTransaction;
import weaql.common.thrift.Status;
import weaql.common.util.exception.SocketConnectionException;


/**
 * @author dnlopes
 *         This interface defines methods for all Proxy communications.
 *         All methods should be thread-safe, because this class is used by multiple threads to send events to
 *         remote nodes
 */
public interface IProxyNetwork
{

	Status commitOperation(CRDTPreCompiledTransaction shadowTransaction) throws SocketConnectionException;
}
