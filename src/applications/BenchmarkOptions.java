package applications;


import weaql.common.util.DatabaseProperties;


/**
 * Created by dnlopes on 15/09/15.
 */
public interface BenchmarkOptions
{

	public String getName();

	public String getJdbc();

	public int getDuration();

	public int getClientsNumber();

	public Workload getWorkload();

	public DatabaseProperties getDbProps();

	public abstract String getDatabaseName();

	public boolean isCRDTDriver();

	public interface JDBCS
	{

		public static String CRDT_DRIVER = "crdt";
		public static String MYSQL_DRIVER = "mysql";
		public static String GALERA_DRIVER = "galera";
		public static String CLUSTER_DRIVER = "cluster";

		public static String[] JDBCS_ALLOWED = {CRDT_DRIVER, MYSQL_DRIVER, GALERA_DRIVER, CLUSTER_DRIVER};
	}


	public interface Defaults
	{

		public static final int RAMPUP_TIME = 1;
	}
}
