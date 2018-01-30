package weaql.server.util;


/**
 * Created by dnlopes on 07/12/15.
 */
public class TransactionCommitFailureException extends Exception
{

	public TransactionCommitFailureException(String message)
	{
		super(message);
	}
}
