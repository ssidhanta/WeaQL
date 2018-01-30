package weaql.server.agents.coordination;


import weaql.common.thrift.CRDTPreCompiledTransaction;
import weaql.common.util.exception.InvalidConfigurationException;
import weaql.common.util.exception.SocketConnectionException;
import weaql.server.util.CompilePreparationException;
import weaql.server.util.CoordinationFailureException;


/**
 * Created by dnlopes on 22/10/15.
 */
public interface CoordinationAgent
{
	void handleCoordination(CRDTPreCompiledTransaction transaction)
			throws CompilePreparationException, CoordinationFailureException, SocketConnectionException, InvalidConfigurationException;
}
