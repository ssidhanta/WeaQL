package applications.micro;


import weaql.common.util.ConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import weaql.common.util.DatabaseProperties;

import java.sql.*;
import java.util.Random;


public class MicroDatabase
{

	static final Logger LOG = LoggerFactory.getLogger(MicroDatabase.class);

	protected Connection conn;
	protected Statement stat;

	final String charSet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
	private Random randomGenerator;

	public MicroDatabase(DatabaseProperties props) throws SQLException, ClassNotFoundException
	{
		this.randomGenerator = new Random(System.currentTimeMillis());
		this.conn = ConnectionFactory.getDefaultConnection(props, "micro");
		this.conn.setAutoCommit(false);
		this.stat = conn.createStatement();
	}

	private String getRandomString(int length)
	{
		StringBuilder rndString = new StringBuilder(length);
		for(int i = 0; i < length; i++)
		{
			rndString.append(charSet.charAt(randomGenerator.nextInt(charSet.length())));
		}
		return rndString.toString();
	}

	public void setupDatabase(boolean useForeignKeys) throws SQLException
	{
		this.createDB();
		this.createTables(useForeignKeys);
		LOG.info("Micro database tables created");
		this.insertIntoTables();
		LOG.info("Micro database tables populated");
		LOG.info("Micro database is ready");
	}

	private void createDB()
	{
		try
		{
			stat.execute("DROP DATABASE IF EXISTS micro");
			conn.commit();
		} catch(SQLException e)
		{
			e.printStackTrace();
		}
		try
		{
			stat.execute("CREATE DATABASE micro");
		} catch(SQLException e)
		{

			e.printStackTrace();
		}

		try
		{
			stat.execute("use micro");
		} catch(SQLException e)
		{

			e.printStackTrace();
		}
		try
		{
			conn.commit();
		} catch(SQLException e)
		{

			e.printStackTrace();
		}

	}

	private void createTables(boolean useForeignKeys) throws SQLException
	{
		int dumb = 0;
		for(int i = 1; i <= MicroConstants.NUMBER_OF_TABLES; i++)
		{
			stat.execute("DROP TABLE IF EXISTS t" + i);
			conn.commit();

			String statement;

			if(i == 0)
				statement = "CREATE TABLE t" + i + " (" +
						"a int(10) NOT NULL, " +
						"b int(10), " +
						"c int(10), " +
						"d int(10), " +
						"e varchar(50) NOT NULL, " +
						"_del BOOLEAN NOT NULL DEFAULT 0, " +
						"_cclock varchar(20) DEFAULT '0', " +
						"_dclock varchar(20) DEFAULT '0', " +
						"PRIMARY KEY(a)" +
						");";
			else if(useForeignKeys)
				statement = "CREATE TABLE t" + i + " (" +
						"a int(10) NOT NULL, " +
						"b int(10), " +
						"c int(10), " +
						"d int(10), " +
						"e varchar(50) NOT NULL, " +
						"_del BOOLEAN NOT NULL DEFAULT 0, " +
						"_cclock varchar(20) DEFAULT '0', " +
						"_dclock varchar(20) DEFAULT '0', " +
						"PRIMARY KEY(a), " +
						"FOREIGN KEY (b) REFERENCES t" + dumb + "(a) ON DELETE CASCADE" +
						");";
			else
				statement = "CREATE TABLE t" + i + " (" +
						"a int(10) NOT NULL, " +
						"b int(10), " +
						"c int(10), " +
						"d int(10), " +
						"e varchar(50) NOT NULL, " +
						"_del BOOLEAN NOT NULL DEFAULT 0, " +
						"_cclock varchar(20) DEFAULT '0', " +
						"_dclock varchar(20) DEFAULT '0', " +
						"PRIMARY KEY(a)" +
						");";

			stat.execute(statement);
			conn.commit();
		}
		conn.commit();
	}

	private void insertIntoTables() throws SQLException
	{
		for(int i = 1; i <= MicroConstants.NUMBER_OF_TABLES; i++)
		{
			for(int j = 0; j < MicroConstants.RECORDS_PER_TABLE; j++)
			{
				int a = j;
				int b = randomGenerator.nextInt(MicroConstants.RECORDS_PER_TABLE) - 1;
				if(i == 1 || i == 2)
					b = 0;

				int c = randomGenerator.nextInt(MicroConstants.RECORDS_PER_TABLE) - 1;
				int d = randomGenerator.nextInt(MicroConstants.RECORDS_PER_TABLE) - 1;
				String e = getRandomString(5);

				String statement = "insert into t" + i + " values (" + Integer.toString(a) + "," + Integer.toString(
						b) + "," +
						Integer.toString(c) + "," + Integer.toString(d) + ",'" + e + "', 0,'0','0')";
				stat.execute(statement);
			}
			conn.commit();
		}
		conn.commit();
	}

}
