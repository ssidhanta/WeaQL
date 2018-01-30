package weaql.common.util;


import weaql.common.database.constraints.Constraint;
import weaql.common.database.table.DatabaseTable;
import weaql.common.database.util.DatabaseMetadata;
import weaql.common.parser.DDLParser;
import weaql.common.util.exception.ConfigurationLoadException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import weaql.server.agents.AgentsFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;


/**
 * Created by dnlopes on 28/10/15.
 */
public class WeaQLEnvironment
{

	private static final Logger LOG = LoggerFactory.getLogger(WeaQLEnvironment.class);

	private volatile static boolean IS_CONFIGURED = false;
	private static WeaQLEnvironment instance;

	public static boolean IS_ZOOKEEPER_REQUIRED = false;
	public static String ENVIRONMENT_FILE;
	public static String DDL_ANNOTATIONS_FILE;
	public static int EZK_CLIENTS_POOL_SIZE;
	public static int REPLICATORS_CONNECTIONS_POOL_SIZE;
	public static int REMOTE_APPLIER_THREAD_COUNT;
	public static int COMMIT_PAD_POOL_SIZE;
	public static boolean OPTIMIZE_BATCH;
	public static String EZK_EXTENSION_CODE;
	public static String DATABASE_NAME;
	public static DatabaseMetadata DB_METADATA;
	public static int DISPATCHER_AGENT;
	public static int DELIVER_AGENT;

	private WeaQLEnvironment(String envFile) throws ConfigurationLoadException
	{
		if(envFile == null)
			throw new ConfigurationLoadException("environment file is null");

		ENVIRONMENT_FILE = envFile;

		loadConfigurations(true);
		loadAnnotationsFile();

		IS_CONFIGURED = true;
	}

	public static WeaQLEnvironment getInstance()
	{
		return instance;
	}

	public static void printEnvironment()
	{
		LOG.info("environment:" + EnvironmentDefaults.DATABASE_NAME_VAR + "=" + WeaQLEnvironment.DATABASE_NAME);
		LOG.info("environment:" + EnvironmentDefaults.DDL_FILE_VAR + "=" + WeaQLEnvironment.DDL_ANNOTATIONS_FILE);
		LOG.info(
				"environment:" + EnvironmentDefaults.COMMIT_PAD_POOL_SIZE_VAR + "=" + WeaQLEnvironment
						.COMMIT_PAD_POOL_SIZE);
		LOG.info("environment:" + EnvironmentDefaults.EZK_EXTENSION_CODE_VAR + "=" + WeaQLEnvironment.EZK_EXTENSION_CODE);
		LOG.info(
				"environment:" + EnvironmentDefaults.EZK_CLIENTS_POOL_SIZE_VAR + "=" + WeaQLEnvironment
						.EZK_CLIENTS_POOL_SIZE);
		LOG.info(
				"environment:" + EnvironmentDefaults.REPLICATORS_CONNECTIONS_POOL_SIZE_VAR + "=" + WeaQLEnvironment
						.REPLICATORS_CONNECTIONS_POOL_SIZE);
		LOG.info("environment:" + EnvironmentDefaults.OPTIMIZE_BATCH_VAR + "=" + WeaQLEnvironment.OPTIMIZE_BATCH);
		LOG.info(
				"environment:" + EnvironmentDefaults.DELIVER_NAME_VAR + "=" + AgentsFactory
						.getDeliverAgentClassAsString());
		LOG.info(
				"environment:" + EnvironmentDefaults.DISPATCHER_NAME_VAR + "=" + AgentsFactory
						.getDispatcherAgentClassAsString());
		LOG.info(
				"environment:" + EnvironmentDefaults.REMOTE_APPLIER_THREADS_COUNT_NAME_VAR + "=" + WeaQLEnvironment
						.REMOTE_APPLIER_THREAD_COUNT);
	}

	public static synchronized void setupEnvironment(String envFile) throws ConfigurationLoadException
	{
		if(IS_CONFIGURED)
			LOG.warn("environment configuration already loaded");
		else
			instance = new WeaQLEnvironment(envFile);
                loadConfigurations(true);
		loadAnnotationsFile();

	}

	private static void loadConfigurations(boolean lookForAnnotationsFile) throws ConfigurationLoadException
	{
		if(ENVIRONMENT_FILE == null)
			throw new ConfigurationLoadException("environment file not set");

		Properties prop = new Properties();

		try
		{
			prop.load(new FileInputStream(ENVIRONMENT_FILE));

			if(prop.containsKey(EnvironmentDefaults.REMOTE_APPLIER_THREADS_COUNT_NAME_VAR))
				WeaQLEnvironment.REMOTE_APPLIER_THREAD_COUNT = Integer.parseInt(
						prop.getProperty(EnvironmentDefaults.REMOTE_APPLIER_THREADS_COUNT_NAME_VAR));
			else
				WeaQLEnvironment.REMOTE_APPLIER_THREAD_COUNT = EnvironmentDefaults.REMOTE_APPLIER_THREADS_COUNT_DEFAULT;

			if(prop.containsKey(WeaQLEnvironment.EnvironmentDefaults.COMMIT_PAD_POOL_SIZE_VAR))
				WeaQLEnvironment.COMMIT_PAD_POOL_SIZE = Integer.parseInt(
						prop.getProperty(WeaQLEnvironment.EnvironmentDefaults.COMMIT_PAD_POOL_SIZE_VAR));
			else
				WeaQLEnvironment.COMMIT_PAD_POOL_SIZE = WeaQLEnvironment.EnvironmentDefaults.COMMIT_PAD_POOL_SIZE_DEFAULT;

			if(prop.containsKey(WeaQLEnvironment.EnvironmentDefaults.REPLICATORS_CONNECTIONS_POOL_SIZE_VAR))
				WeaQLEnvironment.REPLICATORS_CONNECTIONS_POOL_SIZE = Integer.parseInt(
						prop.getProperty(WeaQLEnvironment.EnvironmentDefaults.REPLICATORS_CONNECTIONS_POOL_SIZE_VAR));
			else
				WeaQLEnvironment.REPLICATORS_CONNECTIONS_POOL_SIZE = EnvironmentDefaults
						.REPLICATORS_CONNECTIONS_POOL_SIZE_DEFAULT;

			if(prop.containsKey(WeaQLEnvironment.EnvironmentDefaults.EZK_CLIENTS_POOL_SIZE_VAR))
				WeaQLEnvironment.EZK_CLIENTS_POOL_SIZE = Integer.parseInt(
						prop.getProperty(WeaQLEnvironment.EnvironmentDefaults.EZK_CLIENTS_POOL_SIZE_VAR));
			else
				WeaQLEnvironment.EZK_CLIENTS_POOL_SIZE = WeaQLEnvironment.EnvironmentDefaults.EZK_CLIENTS_POOL_SIZE_DEFAULT;

			if(prop.containsKey(WeaQLEnvironment.EnvironmentDefaults.OPTIMIZE_BATCH_VAR))
				WeaQLEnvironment.OPTIMIZE_BATCH = Boolean.parseBoolean(
						prop.getProperty(WeaQLEnvironment.EnvironmentDefaults.OPTIMIZE_BATCH_VAR));
			else
				WeaQLEnvironment.OPTIMIZE_BATCH = WeaQLEnvironment.EnvironmentDefaults.OPTIMIZE_BATCH_DEFAULT;

			if(prop.containsKey(WeaQLEnvironment.EnvironmentDefaults.DISPATCHER_NAME_VAR))
				WeaQLEnvironment.DISPATCHER_AGENT = Integer.parseInt(
						prop.getProperty(WeaQLEnvironment.EnvironmentDefaults.DISPATCHER_NAME_VAR));
			else
				WeaQLEnvironment.DISPATCHER_AGENT = WeaQLEnvironment.EnvironmentDefaults.DISPATCHER_AGENT_DEFAULT;

			if(prop.containsKey(WeaQLEnvironment.EnvironmentDefaults.DELIVER_NAME_VAR))
				WeaQLEnvironment.DELIVER_AGENT = Integer.parseInt(
						prop.getProperty(WeaQLEnvironment.EnvironmentDefaults.DELIVER_NAME_VAR));
			else
				WeaQLEnvironment.DELIVER_AGENT = WeaQLEnvironment.EnvironmentDefaults.DELIVER_AGENT_DEFAULT;

			if(prop.containsKey(WeaQLEnvironment.EnvironmentDefaults.DATABASE_NAME_VAR))
				WeaQLEnvironment.DATABASE_NAME = prop.getProperty(WeaQLEnvironment.EnvironmentDefaults.DATABASE_NAME_VAR);
			else
				throw new ConfigurationLoadException("missing mandatory 'dbname' parameter in environment file");
			if(prop.containsKey(WeaQLEnvironment.EnvironmentDefaults.EZK_EXTENSION_CODE_VAR))
				WeaQLEnvironment.EZK_EXTENSION_CODE = prop.getProperty(
						WeaQLEnvironment.EnvironmentDefaults.EZK_EXTENSION_CODE_VAR);
			else
				throw new ConfigurationLoadException("missing mandatory 'ezk-extension-code-dir' in environment file");

			if(lookForAnnotationsFile)
			{
				if(prop.containsKey(EnvironmentDefaults.DDL_FILE_VAR))
					WeaQLEnvironment.DDL_ANNOTATIONS_FILE = prop.getProperty(WeaQLEnvironment.EnvironmentDefaults.DDL_FILE_VAR);
				else
					throw new ConfigurationLoadException("missing mandatory 'ddl-file' in environment file");
			}

		} catch(IOException e)
		{
			throw new ConfigurationLoadException(e.getMessage());
		}
	}

	private static void loadAnnotationsFile() throws ConfigurationLoadException
	{
		if(DDL_ANNOTATIONS_FILE == null)
			throw new ConfigurationLoadException("ddl annotations file not set");

		DDLParser parser = new DDLParser(DDL_ANNOTATIONS_FILE);
		DB_METADATA = parser.parseAnnotations();
                LOG.trace("ddl annotations file loaded: {}", DDL_ANNOTATIONS_FILE);

		for(DatabaseTable table : DB_METADATA.getAllTables())
		{
			for(Constraint c : table.getUniqueConstraints())
			{
				if(c.requiresCoordination())
				{
					IS_ZOOKEEPER_REQUIRED = true;
					return;
				}
			}
			for(Constraint c : table.getAutoIncrementConstraints())
			{
				if(c.requiresCoordination())
				{
					IS_ZOOKEEPER_REQUIRED = true;
					return;
				}
			}
			for(Constraint c : table.getCheckConstraints())
			{
				//TODO
				// after proper implementation of check constraints
				// uncomment the next block
				/*
				if(c.requiresCoordination())
				{
					IS_ZOOKEEPER_REQUIRED = true;
					return;
				}           */
			}
		}
	}

	public interface EnvironmentDefaults
	{

		int EZK_CLIENTS_POOL_SIZE_DEFAULT = 20;
		int REMOTE_APPLIER_THREADS_COUNT_DEFAULT = 1;
		int REPLICATORS_CONNECTIONS_POOL_SIZE_DEFAULT = 50;
		int COMMIT_PAD_POOL_SIZE_DEFAULT = 50;
		boolean OPTIMIZE_BATCH_DEFAULT = false;
		int DISPATCHER_AGENT_DEFAULT = 2;
		int DELIVER_AGENT_DEFAULT = 1;

		String DDL_FILE_VAR = "ddl-file";
		String REPLICATORS_CONNECTIONS_POOL_SIZE_VAR = "replicators-con-pool-size";
		String EZK_CLIENTS_POOL_SIZE_VAR = "ezk-client-pool-size";
		String COMMIT_PAD_POOL_SIZE_VAR = "commit-pool-size";
		String OPTIMIZE_BATCH_VAR = "optimize-batch";
		String EZK_EXTENSION_CODE_VAR = "ezk-extension-code-dir";
		String DATABASE_NAME_VAR = "dbname";
		String DISPATCHER_NAME_VAR = "dispatcher";
		String REMOTE_APPLIER_THREADS_COUNT_NAME_VAR = "applierthread";
		String DELIVER_NAME_VAR = "deliver";
	}
}
