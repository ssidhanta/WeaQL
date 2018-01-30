package applications.tpcc;


import applications.BaseBenchmarkOptions;
import applications.Workload;
import weaql.common.util.DatabaseProperties;


/**
 * Created by dnlopes on 10/09/15.
 */
public final class TpccBenchmarkOptions extends BaseBenchmarkOptions
{

	private static final String ORIGINAL_DB_NAME = "tpcc";
	private final String databaseName;

	public TpccBenchmarkOptions(int clientsNumber, int duration, String jdbc, String name, Workload workload,
								DatabaseProperties dbProps)
	{
		super(clientsNumber, duration, jdbc, name, workload, dbProps);

		if(jdbc.compareTo(JDBCS.CRDT_DRIVER) == 0)
			this.databaseName = ORIGINAL_DB_NAME + "_crdt";
		else
			this.databaseName = ORIGINAL_DB_NAME;
	}

	@Override
	public String getDatabaseName()
	{
		return this.databaseName;
	}
}
