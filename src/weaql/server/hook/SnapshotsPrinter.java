package weaql.server.hook;


import weaql.server.replicator.Replicator;


/**
 * Created by dnlopes on 04/01/16.
 */
public class SnapshotsPrinter extends Thread
{

	private final Replicator replicator;


	public SnapshotsPrinter(Replicator replicator)
	{
		this.replicator = replicator;
	}



	@Override
	public void run()
	{

	}
}
