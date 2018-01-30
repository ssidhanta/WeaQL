package applications.util;


/**
 * Created by dnlopes on 03/12/15.
 */
public final class TransactionRecord
{

	private final String txnName;
	private final double execLatency;
	private final double commitLatency;
	private final boolean success;
	private final int iteration;

	public TransactionRecord(String txnName, long execLatency, long commitLAtency, boolean committed, int iteration)
	{
		this.iteration = iteration;
		this.txnName = txnName;
		this.success = committed;
		this.execLatency = execLatency * 0.000001;
		this.commitLatency = commitLAtency * 0.000001;
	}

	public TransactionRecord(String txnName, boolean committed, int iteration)
	{
		this.iteration = iteration;
		this.txnName = txnName;
		this.execLatency = Long.MAX_VALUE;
		this.commitLatency = Long.MAX_VALUE;
		this.success = committed;
	}

	public double getCommitLatency()
	{
		return commitLatency;
	}

	public double getExecLatency()
	{
		return execLatency;
	}

	public String getTxnName()
	{
		return txnName;
	}

	public double getTotalLatency()
	{
		return commitLatency + execLatency;
	}

	public boolean isSuccess()
	{
		return this.success;
	}

	public int getIteration()
	{
		return iteration;
	}

}
