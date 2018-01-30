package weaql.tests.parser;


import weaql.common.database.util.DatabaseMetadata;
import weaql.common.util.WeaQLEnvironment;


/**
 * Created by dnlopes on 06/03/15.
 */
public class ParserTest
{

	private static final String SCHEMA_FILE =
			"/Users/dnlopes/workspaces/thesis/code/weakdb/resources/configs/tpcc_localhost_1node.xml";

	public static void main(String args[])
	{
		System.setProperty("configPath", SCHEMA_FILE);
		DatabaseMetadata metadata = WeaQLEnvironment.DB_METADATA;

		int a = 0;
	}

}
