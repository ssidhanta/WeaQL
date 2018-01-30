package applications.tpcc;


import applications.BaseBenchmarkOptions;
import weaql.common.util.Topology;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.List;


/**
 * Created by dnlopes on 17/12/15.
 */
public class TpccHook extends Thread
{

	private List<TPCCClientEmulator> clients;
	private BaseBenchmarkOptions options;
	private int emulatorId;

	public TpccHook(List<TPCCClientEmulator> clients, BaseBenchmarkOptions options, int emulatorId)
	{
		this.clients = clients;
		this.options = options;
		this.emulatorId = emulatorId;
	}

	@Override
	public void run()
	{
		TPCCStatistics globalStats = new TPCCStatistics(0);

		for(TPCCClientEmulator client : this.clients)
		{
			TPCCStatistics partialStats = client.getStats();
			globalStats.mergeStatistics(partialStats);
		}

		globalStats.generateStatistics();
		String statsString = globalStats.getStatsString();
		String distributionStrings = globalStats.getDistributionStrings();

		PrintWriter out;

		StringBuffer buffer = new StringBuffer();
		StringBuffer distributionBuffer = new StringBuffer();
		buffer.append("txn_name").append(",");
		buffer.append("commits").append(",");
		buffer.append("aborts").append(",");
		buffer.append("maxLatency").append(",");
		buffer.append("minLatency").append(",");
		buffer.append("avgLatency").append(",");
		buffer.append("avgExecLatency").append(",");
		buffer.append("avgCommitLatency").append(",");
		buffer.append("txnPerSecond").append("\n");
		buffer.append(statsString).append("\n");

		distributionBuffer.append(distributionStrings).append("\n");

		String outputContent = buffer.toString();
		String distributionContent = distributionBuffer.toString();

		try
		{
			String fileName = Topology.getInstance().getReplicatorsCount() + "_replicas_" + 5 * options
					.getClientsNumber() + "_users_" + options.getJdbc() + "_jdbc_emulator" + this.emulatorId + ".csv";
			String distributionFileName = Topology.getInstance().getReplicatorsCount() + "_replicas_" + options
					.getClientsNumber() * 5 + "_users_" + options.getJdbc() + "_jdbc_emulator" + this.emulatorId +
					"_distribution.log";

			out = new PrintWriter(fileName);
			out.write(outputContent);
			out.close();
			out = new PrintWriter(distributionFileName);
			out.write(distributionContent);
			out.close();
		} catch(FileNotFoundException e)
		{
			e.printStackTrace();
		}
	}
}
