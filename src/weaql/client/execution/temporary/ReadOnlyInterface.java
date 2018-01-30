package weaql.client.execution.temporary;


import weaql.client.operation.SQLSelect;

import java.sql.ResultSet;
import java.sql.SQLException;


/**
 *
 * Created by dnlopes on 16/09/15.
 * A Scratchpad is a representation of the database using in-memory tables only.
 * It is the actual implementation of the sandbox environment.
 * A scratchpad does not actually execute SQL statements on temporary tables.
 * Instead, each scratchpad owns a list of IExecutorAgent objects, and throw them, sql statements are executed.
 *
 * This class exposes only a single operation, and it is used with read-only transactions.
 *
 */
public interface ReadOnlyInterface
{
	ResultSet executeQuery(SQLSelect query) throws SQLException;
	ResultSet executeQuery(String query) throws SQLException;
}
