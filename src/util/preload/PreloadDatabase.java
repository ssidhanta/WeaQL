package util.preload;


import weaql.common.util.ConnectionFactory;
import weaql.common.util.DatabaseProperties;
import weaql.common.util.defaults.DatabaseDefaults;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;


/**
 * Created by dnlopes on 15/12/15.
 */
public class PreloadDatabase
{
	public static void main(String args[]) throws SQLException
	{

		if(args.length != 2)
		{
			System.err.println("usage: java -jar <jar name> <database host> <database name>");
			System.exit(-1);
		}


		String host = args[0];
		String dbName = args[1];
		DatabaseProperties dbProps = new DatabaseProperties(DatabaseDefaults.DEFAULT_USER, DatabaseDefaults
				.DEFAULT_PASSWORD, host, 3306);

		PreloadDatabase loader = new PreloadDatabase();
		loader.loadIntoMemory(dbName, dbProps);
	}

	public void loadIntoMemory(String databaseName,DatabaseProperties dbProps) throws SQLException
	{

		Connection connection = ConnectionFactory.getDefaultConnection(dbProps, databaseName);

		Statement stat = connection.createStatement();

		stat.execute("SELECT COUNT(*) from warehouse");
		stat.execute("SELECT COUNT(*) from district");
		stat.execute("SELECT COUNT(*) from customer");
		stat.execute("SELECT COUNT(*) from history");
		stat.execute("SELECT COUNT(*) from orders");
		stat.execute("SELECT COUNT(*) from new_orders");
		stat.execute("SELECT COUNT(*) from stock");
		stat.execute("SELECT COUNT(*) from item");
		stat.execute("SELECT COUNT(*) from order_line");
		stat.execute("SELECT COUNT(*) FROM customer FORCE INDEX(ix_customer)");
		stat.execute("SELECT COUNT(*) FROM order_line FORCE INDEX(ix_order_line)");
		stat.execute("SELECT COUNT(*) FROM orders FORCE INDEX(pk_orders)");
		stat.execute("SELECT COUNT(*) FROM orders FORCE INDEX(ix_orders)");
		stat.execute("SELECT COUNT(*) FROM new_orders FORCE INDEX(ix_new_orders)");

		stat.close();
	}
}
