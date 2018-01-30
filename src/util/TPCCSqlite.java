package util;

import java.sql.*;

public class TPCCSqlite
{
	public static void main( String args[] )
	{
		if(args.length != 2)
		{
			System.err.println("usage: java -jar <jar name> <database host> <database name>");
			System.exit(-1);
		}

		String dbHost = args[0];
		String dbName = args[1];

		Connection c = null;
		try {
			Class.forName("org.sqlite.JDBC");
			c = DriverManager.getConnection("jdbc:sqlite:"+dbName+".db");
			Statement stat = c.createStatement();
			stat.execute("insert into t1 values (5,5)");
			c.commit();
			System.out.println("inserted new row");
		} catch ( Exception e ) {
			System.err.println( e.getClass().getName() + ": " + e.getMessage() );
			System.exit(0);
		}

	}
}