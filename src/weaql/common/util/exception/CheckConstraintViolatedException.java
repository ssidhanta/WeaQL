package weaql.common.util.exception;


import java.sql.SQLException;


/**
 * Created by dnlopes on 23/03/15.
 */
public class CheckConstraintViolatedException extends SQLException
{

	public CheckConstraintViolatedException()
	{
		super("check constraint violated");
	}

	public CheckConstraintViolatedException(String arg0)
	{
		super(arg0);
	}

	public CheckConstraintViolatedException(Throwable arg0)
	{
		super(arg0);
	}


}