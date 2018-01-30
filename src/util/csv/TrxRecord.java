package util.csv;


/**
 * Created by dnlopes on 10/12/15.
 */
public class TrxRecord
{

	String name;
	int commits;
	int clients;
	int aborts;
	double maxLatency;
	double minLatency;
	double avgLatency;
	double avgExecLatency;
	double avgCommitLatency;

	public void setName(String name)
	{
		this.name = name;
	}

	public String getName()
	{
		return name;
	}

	public int getCommits()
	{
		return commits;
	}

	public int getAborts()
	{
		return aborts;
	}

	public double getMaxLatency()
	{
		return maxLatency;
	}

	public double getMinLatency()
	{
		return minLatency;
	}

	public double getAvgLatency()
	{
		return avgLatency;
	}

	public double getAvgExecLatency()
	{
		return avgExecLatency;
	}

	public double getAvgCommitLatency()
	{
		return avgCommitLatency;
	}

	public int getClients()
	{
		return clients;
	}

	public void setClients(int clients)
	{
		this.clients = clients;
	}

	public TrxRecord(int clients, String name, int commits, int aborts, double maxLatency, double minLatency,
					 double avgLatency, double avgExecLatency, double avgCommitLatency)
	{
		this.clients = clients;
		this.name = name;
		this.commits = commits;
		this.aborts = aborts;
		this.maxLatency = maxLatency;
		this.minLatency = minLatency;
		this.avgLatency = avgLatency;
		this.avgExecLatency = avgExecLatency;
		this.avgCommitLatency = avgCommitLatency;
	}

	public TrxRecord(String name, int commits, int aborts, double maxLatency, double minLatency, double avgLatency,
					 double avgExecLatency, double avgCommitLatency)
	{
		this.name = name;
		this.commits = commits;
		this.aborts = aborts;
		this.maxLatency = maxLatency;
		this.minLatency = minLatency;
		this.avgLatency = avgLatency;
		this.avgExecLatency = avgExecLatency;
		this.avgCommitLatency = avgCommitLatency;
	}
}
