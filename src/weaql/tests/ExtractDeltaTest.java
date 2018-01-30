package weaql.tests;


import weaql.common.database.util.DatabaseCommon;

import java.sql.SQLException;


/**
 * Created by dnlopes on 22/12/15.
 */
public class ExtractDeltaTest
{

	public static void main(String args[]) throws SQLException
	{
		String delta = "a+1";
		String delta1 = "a + 1";
		String delta2 = "a - 1";
		String delta3 = "a-1";


		double rest = DatabaseCommon.extractDelta(delta, "a");
		rest = DatabaseCommon.extractDelta(delta1, "a");
		rest = DatabaseCommon.extractDelta(delta2, "a");
		rest = DatabaseCommon.extractDelta(delta3, "a");
	}
}
