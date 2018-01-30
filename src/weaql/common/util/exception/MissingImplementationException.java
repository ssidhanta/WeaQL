package weaql.common.util.exception;


import java.sql.SQLException;


/**
 * Created by dnlopes on 11/03/15.
 */
public class MissingImplementationException extends SQLException
{

	public MissingImplementationException()
	{
	}

	public MissingImplementationException(String arg0)
	{
		super(arg0);
	}

	public MissingImplementationException(Throwable arg0)
	{
		super(arg0);
	}

}