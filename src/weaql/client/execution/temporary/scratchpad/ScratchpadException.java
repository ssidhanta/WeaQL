package weaql.client.execution.temporary.scratchpad;


import java.sql.SQLException;


/**
 * Created by dnlopes on 10/03/15.
 */

public class ScratchpadException extends SQLException
{

	public ScratchpadException()
	{
	}

	public ScratchpadException(String arg0)
	{
		super(arg0);
	}

	public ScratchpadException(Throwable arg0)
	{
		super(arg0);
	}

	public ScratchpadException(String arg0, Throwable arg1) {
		super(arg0, arg1);
	}
}

