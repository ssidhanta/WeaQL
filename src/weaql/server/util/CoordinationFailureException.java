package weaql.server.util;


import java.sql.SQLException;


/**
 * Created by dnlopes on 09/12/15.
 */
public class CoordinationFailureException extends SQLException
{

	public CoordinationFailureException(String message)
	{
		super(message);
	}
}
