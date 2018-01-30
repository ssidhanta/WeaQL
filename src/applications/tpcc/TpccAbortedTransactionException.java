package applications.tpcc;


/**
 * Created by dnlopes on 15/09/15.
 */
public class TpccAbortedTransactionException extends Exception
{
	public TpccAbortedTransactionException() {
		super();
	}

	public TpccAbortedTransactionException(String message) {
		super(message);
	}
}
