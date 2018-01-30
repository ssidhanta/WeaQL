package applications.tpcc;


import applications.TransactionStats;
import applications.util.TransactionRecord;

import java.util.*;


/**
 * Created by dnlopes on 26/05/15.
 */
public class TPCCStatistics
{

	private Map<String, List<TransactionRecord>> txnRecords;
	private List<TransactionStats> txnStats;
	private Map<String, Map<Integer, Integer>> recordsPerIteration;

	public TPCCStatistics(int id)
	{
		this.txnRecords = new HashMap<>();
		this.recordsPerIteration = new HashMap<>();
		this.txnStats = new ArrayList<>();

		for(String txnName : TpccConstants.TXNS_NAMES)
		{
			this.txnRecords.put(txnName, new LinkedList<TransactionRecord>());
			this.recordsPerIteration.put(txnName, new HashMap<Integer, Integer>());

			for(int i = 0; i < 100; i++)
				recordsPerIteration.get(txnName).put(i, 0);
		}
	}

	public void addTxnRecord(String txnName, TransactionRecord record)
	{
		this.txnRecords.get(txnName).add(record);

		int currentCounter = this.recordsPerIteration.get(txnName).get(record.getIteration());
		currentCounter++;
		this.recordsPerIteration.get(txnName).put(record.getIteration(), currentCounter);
	}

	public void mergeStatistics(TPCCStatistics otherStats)
	{
		for(Map.Entry<String, List<TransactionRecord>> entry : otherStats.getTxnRecords().entrySet())
		{
			this.txnRecords.get(entry.getKey()).addAll(entry.getValue());
		}

		for(Map.Entry<String, Map<Integer, Integer>> entry : otherStats.getRecordsPerIteration().entrySet())
		{
			String txnName = entry.getKey();

			Map<Integer, Integer> otherPerSecondStats = entry.getValue();
			Map<Integer, Integer> myPerSecondStats = recordsPerIteration.get(txnName);

			for(Map.Entry<Integer, Integer> entry1 : otherPerSecondStats.entrySet())
			{
				int iteration = entry1.getKey();

				if(iteration > TPCCEmulator.DURATION)
					break;

				int otherCounter = entry1.getValue();

				int myCounter = myPerSecondStats.get(iteration);
				int sum = myCounter + otherCounter;
				recordsPerIteration.get(txnName).put(iteration, sum);
			}
		}
	}

	public Map<String, List<TransactionRecord>> getTxnRecords()
	{
		return this.txnRecords;
	}

	public void generateStatistics()
	{
		for(Map.Entry<String, List<TransactionRecord>> entry : this.txnRecords.entrySet())
		{
			TransactionStats txnStats = new TransactionStats(entry.getKey(), entry.getValue(),
					recordsPerIteration.get(entry.getKey()));
			this.txnStats.add(txnStats);
		}
	}

	public String getStatsString()
	{
		StringBuilder buffer = new StringBuilder();

		for(TransactionStats txnStat : this.txnStats)
			buffer.append(txnStat.getStatsString());

		return buffer.toString();
	}

	public String getStatsString(int iteration)
	{
		StringBuilder buffer = new StringBuilder();

		for(TransactionStats txnStat : this.txnStats)
			buffer.append(txnStat.getStatsString(iteration));

		return buffer.toString();
	}

	public String getDistributionStrings()
	{
		StringBuilder buffer = new StringBuilder();

		for(TransactionStats txnStat : this.txnStats)
			buffer.append(txnStat.getDistributionStatsString());

		return buffer.toString();
	}

	public Map<String, Map<Integer, Integer>> getRecordsPerIteration()
	{
		return recordsPerIteration;
	}

}
