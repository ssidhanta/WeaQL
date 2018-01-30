package weaql.server.util;


import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;


/**
 * Created by dnlopes on 28/10/15.
 */
public class StatsCollector
{

	private AtomicInteger commitsCounter;
	private AtomicInteger retriesCounter;
	private AtomicInteger abortsCounter;
	private AtomicLong sumLatency;

	public StatsCollector()
	{
		this.commitsCounter = new AtomicInteger();
		this.retriesCounter = new AtomicInteger();
		this.abortsCounter = new AtomicInteger();
		this.sumLatency = new AtomicLong();
	}

	public void addLatency(long latency)
	{
		this.sumLatency.addAndGet(latency);
	}

	public long getAverageLatency()
	{
		return this.sumLatency.get() / commitsCounter.get();
	}

	public void incrementCommits()
	{
		this.commitsCounter.incrementAndGet();
	}

	public void incrementRetries()
	{
		this.retriesCounter.incrementAndGet();
	}

	public void incrementAborts()
	{
		this.abortsCounter.incrementAndGet();
	}

	public int getCommitsCounter()
	{
		return commitsCounter.get();
	}

	public int getRetriesCounter()
	{
		return retriesCounter.get();
	}

	public int getAbortsCounter()
	{
		return abortsCounter.get();
	}
}
