package com.codefutures.tpcc.stats;


/**
 * Created by dnlopes on 04/06/15.
 */
public class PerformanceCounters
{

	private long performanceRefreshInterval = 10000L;
	public final float MINIMUM_VALUE = 0.05F;

	private float inCommingRate = 0F;
	private float commitRate = 0F;
	private float abortRate = 0F;
	private double latencyRate = 0F;
	private int inCommingCounter = 0;
	private int abortCounter = 0;
	private int commitCounter = 0;
	private int totalNewOrderCommitCounter = 0;
	private long lastComputationInComming, lastComputationAbort, lastComputationCommit = 0, lastComputationLatency = 0;
	private long firstNewOrderCommit = -1;
	private int totalAbortCounter = 0;
	private int totalCommitCounter = 0;
	private double latencyAccumulator = 0;

	public PerformanceCounters()
	{
	}

	public float getAbortRate()
	{
		long current = System.currentTimeMillis();
		long diff = current - lastComputationAbort;
		float t = abortRate;

		if(diff > performanceRefreshInterval && diff > 0)
		{
			t = ((float) abortCounter / (float) (diff)) * 1000;
			t = (t < MINIMUM_VALUE ? 0 : t);
			lastComputationAbort = current;
			abortCounter = 0;
		}
		abortRate = t;

		return (abortRate);
	}

	public float getCommitRate()
	{
		long current = System.currentTimeMillis();
		long diff = current - lastComputationCommit;
		float t = commitRate;

		if(diff > performanceRefreshInterval && diff > 0)
		{
			t = ((float) commitCounter / (float) (diff)) * 1000;
			t = (t < MINIMUM_VALUE ? 0 : t);
			lastComputationCommit = current;
			commitCounter = 0;
		}
		commitRate = t;

		return (commitRate);
	}

	public float getTotalNewOrderCommitRate()
	{
		long current = System.currentTimeMillis();
		long diff = current - firstNewOrderCommit;
		float t = ((float) totalNewOrderCommitCounter / (float) (diff)) * 1000 * 60;
		t = (t < MINIMUM_VALUE ? 0 : t);
		return (t);
	}

	public float getTotalAbortRate()
	{
		return ((float) totalAbortCounter * 1.0f) / ((float) totalAbortCounter + totalCommitCounter);

	}

	public float getIncommingRate()
	{
		long current = System.currentTimeMillis();
		long diff = current - lastComputationInComming;
		float t = inCommingRate;

		if(diff > performanceRefreshInterval && diff > 0)
		{
			t = ((float) inCommingCounter / (float) (diff)) * 1000;
			t = (t < MINIMUM_VALUE ? 0 : t);
			lastComputationInComming = current;
			inCommingCounter = 0;
		}
		inCommingRate = t;

		return (inCommingRate);
	}

	public double getAverageLatency()
	{

		long current = System.currentTimeMillis();
		long diff = current - lastComputationLatency;
		double t = this.latencyRate;

		if(diff > performanceRefreshInterval && diff > 0)
		{
			if(this.latencyCounter > 0)
			{
				t = ((double) this.latencyAccumulator) / ((double) this.latencyCounter);
				t = (t < MINIMUM_VALUE ? 0 : t);
			} else
			{
				t = 0.0;
			}

			this.lastComputationLatency = current;
			this.latencyCounter = 0;
			this.latencyAccumulator = 0;
		}
		latencyRate = t;

		return this.latencyRate;
	}

	public void setIncommingRate()
	{
		
		inCommingCounter++;
		
	}

	public void setAbortRate()
	{
		
		abortCounter++;
		totalAbortCounter++;
		
	}

	public void setCommitRate()
	{
		
		commitCounter++;
		totalCommitCounter++;
		
	}

	public void setTPMC()
	{
		
		if(firstNewOrderCommit < 0)
		{
			firstNewOrderCommit = System.currentTimeMillis();
		}
		totalNewOrderCommitCounter++;
	}

	private double latencyCounter = 0;

	public void setLatency(double latency)
	{
		
		latencyAccumulator += latency;
		latencyCounter++;
		
	}

	public long getPerformanceRefreshInterval()
	{
		return this.performanceRefreshInterval;
	}

	public void setPerformanceRefreshInterval(long refreshInterval)
	{
		this.performanceRefreshInterval = refreshInterval;
	}

	public int getCommitCounter()
	{
		return this.commitCounter;
	}
}
