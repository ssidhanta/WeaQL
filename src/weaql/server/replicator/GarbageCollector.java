package weaql.server.replicator;


import weaql.common.database.SQLBasicInterface;
import weaql.common.database.SQLInterface;
import weaql.common.database.util.DatabaseMetadata;
import weaql.common.database.table.DatabaseTable;
import weaql.common.util.WeaQLEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import weaql.server.util.LogicalClock;
import weaql.common.util.defaults.ReplicatorDefaults;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;


/**
 * Created by dnlopes on 10/09/15.
 */
public class GarbageCollector implements Runnable
{

	private static final Logger LOG = LoggerFactory.getLogger(GarbageCollector.class);

	private SQLInterface sqlInterface;
	private Replicator replicator;
	private List<DatabaseTable> toCollectTabes;

	public GarbageCollector(Replicator replicator)
	{
		this.replicator = replicator;
		this.toCollectTabes = new ArrayList<>();

		try
		{
			this.sqlInterface = new SQLBasicInterface(replicator.getConfig());

		} catch(SQLException e)
		{
			if(LOG.isWarnEnabled())
				LOG.warn("failed to create connection for GarbageCollector agent", e);
		}

		if(LOG.isInfoEnabled())
			LOG.info("garbage collection agent initialized with schedule interval of {}",
					ReplicatorDefaults.GARBAGE_COLLECTOR_THREAD_INTERVAL);

		DatabaseMetadata metadata = WeaQLEnvironment.DB_METADATA;

		for(DatabaseTable dbTable : metadata.getAllTables())
		{
			if(dbTable.getTablePolicy().allowDeletes())
				toCollectTabes.add(dbTable);
		}
	}

	@Override
	public void run()
	{
		LogicalClock clockSnapshot = this.replicator.getCurrentClock().duplicate();
		//TODO: implement
	}
}
