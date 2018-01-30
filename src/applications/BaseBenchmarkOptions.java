package applications;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import weaql.common.util.DatabaseProperties;


/**
 * Created by dnlopes on 10/09/15.
 */
public abstract class BaseBenchmarkOptions implements BenchmarkOptions
{

	private static final Logger LOG = LoggerFactory.getLogger(BaseBenchmarkOptions.class);

	private final int clientsNumber;
	private final int duration;
	private final String jdbc;
	private final String name;
	private final Workload workload;
	private final DatabaseProperties dbProps;

	public BaseBenchmarkOptions(int clientsNumber, int duration, String jdbc, String name, Workload workload,
								DatabaseProperties dbProps)
	{
		this.clientsNumber = clientsNumber;
		this.duration = duration;
		this.jdbc = jdbc;
		this.name = name;
		this.workload = workload;
		this.dbProps = dbProps;

		if(!isValidJdbc(this.jdbc))
		{
			LOG.error("invalid jdbc");
			System.exit(-1);
		}
	}

	@Override
	public String getName()
	{
		return this.name;
	}

	@Override
	public String getJdbc()
	{
		return this.jdbc;
	}

	@Override
	public int getDuration()
	{
		return this.duration;
	}

	@Override
	public int getClientsNumber()
	{
		return this.clientsNumber;
	}

	@Override
	public Workload getWorkload()
	{
		return this.workload;
	}

	@Override
	public DatabaseProperties getDbProps()
	{
		return this.dbProps;
	}

	@Override
	public boolean isCRDTDriver()
	{
		return this.jdbc.compareTo(BenchmarkOptions.JDBCS.CRDT_DRIVER) == 0;
	}

	@Override
	public abstract String getDatabaseName();

	private boolean isValidJdbc(String jdbc)
	{
		for(String driver : BenchmarkOptions.JDBCS.JDBCS_ALLOWED)
		{
			if(jdbc.compareTo(driver) == 0)
				return true;
		}

		return false;
	}

}
