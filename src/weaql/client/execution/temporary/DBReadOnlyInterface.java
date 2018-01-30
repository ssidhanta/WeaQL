package weaql.client.execution.temporary;


import weaql.client.operation.SQLOperationType;
import weaql.client.operation.SQLSelect;
import weaql.common.database.SQLInterface;

import java.sql.ResultSet;
import java.sql.SQLException;


/**
 * Created by dnlopes on 16/09/15.
 */
public class DBReadOnlyInterface implements ReadOnlyInterface
{

	private SQLInterface sqlInterface;

	public DBReadOnlyInterface(SQLInterface sqlInterface)
	{
		this.sqlInterface = sqlInterface;
	}

	@Override
	public ResultSet executeQuery(SQLSelect selectSQL) throws SQLException
	{

		if(selectSQL.getOpType() != SQLOperationType.SELECT)
			throw new SQLException("query statement expected");

		selectSQL.prepareOperation();

		return this.sqlInterface.executeQuery(selectSQL.getSQLString());
	}

	@Override
	public ResultSet executeQuery(String query) throws SQLException
	{
		return this.sqlInterface.executeQuery(query);
	}
}
