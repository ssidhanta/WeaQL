package weaql.util.dbtransform;

import weaql.common.util.ExitCode;


/**
 * Created by dnlopes on 16/06/15.
 */
public class DatabaseTransformerMain
{


	public static void main(String args[])
	{
		if(args.length != 2)
		{
			System.out.println("usage: java -jar <jarfile> <databaseHost> <databaseName>");
			System.exit(ExitCode.WRONG_ARGUMENTS_NUMBER);
		}

		String dbHost = args[0];
		String dbName = args[1];

		System.out.println("preparing database " + dbName + " at " + dbHost);

		DatabaseTransformer transformer = new DatabaseTransformer(dbHost, dbName);
		transformer.transformDatabase();

		System.out.println("success!");
	}
}
