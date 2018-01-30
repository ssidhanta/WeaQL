package applications;


import applications.tpcc.TPCCEmulator;
import applications.tpcc.TpccConstants;
import applications.util.TransactionRecord;

import java.util.List;
import java.util.Map;


/**
 * Created by dnlopes on 03/12/15.
 */
public class TransactionStats
{

	private final String txnName;
	private final List<TransactionRecord> records;
	protected long successCounter;
	protected long abortsCounter;
	protected double totalLatencySum, execLatencySum, commitLatencySum;
	protected double maxLatency, minLatency, avgLatency, avgExecLatency, avgCommitLatency;
	private int[] latencyDistribution;
	private Map<Integer,Integer> perSecondStats;
	private double txnPerSecond;

	public TransactionStats(String txnName, List<TransactionRecord> txnRecords, Map<Integer,Integer> perSecondStats)
	{
		this.txnName = txnName;
		this.records = txnRecords;
		this.perSecondStats = perSecondStats;
		this.txnPerSecond = 0;

		this.latencyDistribution = new int[TpccConstants.BUCKETS_NUMBER];
		this.successCounter = 0;
		this.abortsCounter = 0;
		this.minLatency = 999.0;
		this.maxLatency = 0;
		this.totalLatencySum = this.execLatencySum = this.commitLatencySum = 0;

		for(TransactionRecord record : this.records)
			addRecord(record);

		calculateStats();
	}

	private void addRecord(TransactionRecord aRecord)
	{
		if(aRecord.isSuccess())
		{
			this.successCounter++;
			this.execLatencySum += aRecord.getExecLatency();
			this.commitLatencySum += aRecord.getCommitLatency();
			this.totalLatencySum += aRecord.getTotalLatency();

			if(aRecord.getTotalLatency() > this.maxLatency)
				this.maxLatency = aRecord.getTotalLatency();

			if(aRecord.getTotalLatency() < this.minLatency)
				this.minLatency = aRecord.getTotalLatency();

			if(aRecord.getTotalLatency() < TpccConstants.LATENCY_INTERVAL * TpccConstants.BUCKETS_NUMBER)
			{
				int indexEntry = (int) Math.floor(aRecord.getTotalLatency() / TpccConstants.LATENCY_INTERVAL);
				if(indexEntry == 0)
				{
					int a = 0;
				} else if(indexEntry == 1)
				{
					int a = 0;
				} else if(indexEntry == 2)
				{
					int a = 0;
				}
				this.latencyDistribution[indexEntry]++;
			} else
				this.latencyDistribution[TpccConstants.BUCKETS_NUMBER - 1]++;

		} else
			this.abortsCounter++;
	}

	private void calculateStats()
	{
		if(this.successCounter == 0)
		{
			this.avgLatency = 999.0;
			this.maxLatency = 999.0;
			this.avgCommitLatency = 999.0;
			this.avgExecLatency = 999.0;
		} else
		{
			this.avgLatency = (this.totalLatencySum) / this.successCounter * 1.0;
			this.avgCommitLatency = (this.commitLatencySum) / this.successCounter * 1.0;
			this.avgExecLatency = (this.execLatencySum) / this.successCounter * 1.0;
		}

		double total = 0;

		for(Map.Entry<Integer, Integer> aSecond : perSecondStats.entrySet())
		{
			if(aSecond.getKey() > TPCCEmulator.DURATION)
				continue;

			total += aSecond.getValue();
		}

		double duration = TPCCEmulator.DURATION;

		txnPerSecond =  total / duration;
	}

	public String getStatsString()
	{
		StringBuffer buffer = new StringBuffer();

		buffer.append(txnName).append(",");
		buffer.append(successCounter).append(",");
		buffer.append(abortsCounter).append(",");
		buffer.append(maxLatency).append(",");
		buffer.append(minLatency).append(",");
		buffer.append(avgLatency).append(",");
		buffer.append(avgExecLatency).append(",");
		buffer.append(avgCommitLatency).append(",");
		buffer.append(txnPerSecond).append("\n");

		return buffer.toString();
	}

	public String getStatsString(int iteration)
	{
		StringBuffer buffer = new StringBuffer();

		buffer.append(iteration).append(",");
		buffer.append(txnName).append(",");
		buffer.append(successCounter).append(",");
		buffer.append(abortsCounter).append(",");
		buffer.append(maxLatency).append(",");
		buffer.append(minLatency).append(",");
		buffer.append(avgLatency).append(",");
		buffer.append(avgExecLatency).append(",");
		buffer.append(avgCommitLatency).append("\n");

		return buffer.toString();
	}

	public String getDistributionStatsString()
	{
		StringBuffer buffer = new StringBuffer();
		buffer.append(txnName).append("\n");

		int lowerBound = 0;
		int higherBound = lowerBound + TpccConstants.LATENCY_INTERVAL - 1;
		int index = 0;

		while(index < this.latencyDistribution.length)
		{
			if(index == this.latencyDistribution.length - 1)
				buffer.append("[" + lowerBound + " ms,+oo]\t");
			else if(index == 0)
				buffer.append("[" + lowerBound + " ms," + higherBound + " ms]\t");
			else
				buffer.append("[" + lowerBound + " ms," + higherBound + " ms]\t");

			buffer.append(this.latencyDistribution[index] + "\n");

			lowerBound += TpccConstants.LATENCY_INTERVAL;
			higherBound = lowerBound + TpccConstants.LATENCY_INTERVAL - 1;
			index++;
		}

		return buffer.append("\n").toString();
	}

}
