package escada.tpc.tpcc.stats;


import java.util.ArrayList;
import java.util.List;


/**
 * Created by dnlopes on 26/05/15.
 */
public class PerSecondStatistics
{
	private int successCounter;
	private int abortsCounter;
	private long maxLatency, minLatency, avgLatency;

	private final int iteration;
	private final List<ThreadStatistics> stats;

	public PerSecondStatistics(int iteration)
	{
		this.iteration = iteration;
		this.stats = new ArrayList<>();
		this.successCounter = 0;
		this.abortsCounter = 0;
		this.maxLatency = 0;
		this.minLatency = 50000;
		this.avgLatency = 0;
	}

	public void addThreadStatistic(ThreadStatistics threadStats)
	{
		this.stats.add(threadStats);
	}

	public void calculateStats()
	{
		for(ThreadStatistics threadStat : this.stats)
			threadStat.calculateMissingStats();

		this.mergeStats();
	}

	private void mergeStats()
	{
		for(ThreadStatistics stats : this.stats)
		{
			successCounter += stats.successCounter;
			abortsCounter += stats.abortsCounter;
			avgLatency += stats.avgLatency;

			if(stats.maxLatency > this.maxLatency)
				this.maxLatency = stats.maxLatency;

			if(stats.minLatency < this.minLatency)
				this.minLatency = stats.minLatency;
		}

		avgLatency = avgLatency / this.stats.size();
	}

	@Override
	public String toString()
	{
		// CSV style: emulatorid,iteration,success,aborts,avgLatency,maxLatency,minLatency
	  	StringBuilder buffer = new StringBuilder();
		buffer.append(this.iteration);
		buffer.append(",");
		buffer.append(this.successCounter);
		buffer.append(",");
		buffer.append(this.abortsCounter);
		buffer.append(",");
		buffer.append(this.avgLatency);
		buffer.append(",");
		buffer.append(this.maxLatency);
		buffer.append(",");
		buffer.append(this.minLatency);

		return buffer.toString();
	}
}
