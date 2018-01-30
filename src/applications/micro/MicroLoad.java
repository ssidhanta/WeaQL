package applications.micro;


import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Random;


/**
 * Created by dnlopes on 22/12/15.
 */
public class MicroLoad
{

	private Connection connection;
	protected Statement stat;

	String dbName;
	final String charSet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
	private Random randomGenerator;

	public MicroLoad(String dbHost, String dbName) throws ClassNotFoundException, SQLException
	{
		this.dbName = dbName;
		Class.forName("com.mysql.jdbc.Driver");
		StringBuilder buffer = new StringBuilder("jdbc:mysql://");
		buffer.append(dbHost);
		buffer.append(":");
		buffer.append(3306);

		this.connection = DriverManager.getConnection(buffer.toString(), "sa", "101010");
		this.connection.setAutoCommit(false);
		this.stat = connection.createStatement();
		this.randomGenerator = new Random(System.nanoTime());
		stat.execute("use " + dbName);
		this.connection.commit();
	}

	public static void main(String[] argv) throws SQLException, ClassNotFoundException
	{
		if(argv.length != 2)
		{
			System.out.println("usage: java -jar <jarFile> <dbHost> <dbName>");
			System.exit(1);
		}

		String dbHost = argv[0];
		String dbName = argv[1];

		MicroLoad loader = new MicroLoad(dbHost, dbName);
		System.out.println("dropping records from " + MicroConstants.NUMBER_OF_TABLES + " tables");
		loader.dropRecords();
		System.out.println(
				"populating " + MicroConstants.NUMBER_OF_TABLES + " tables with " + MicroConstants.RECORDS_PER_TABLE +
						" records each");
		loader.populate();
		System.out.println("database " + dbName + " is ready!");

	}

	public void dropRecords() throws SQLException
	{
		for(int i = 1; i <= MicroConstants.NUMBER_OF_TABLES; i++)
		{
			String statement = "DELETE FROM t" + i;
			stat.execute(statement);
		}
		connection.commit();
	}

	public void populate() throws SQLException
	{
		for(int i = 1; i <= MicroConstants.NUMBER_OF_TABLES; i++)
		{
			for(int j = 0; j < MicroConstants.RECORDS_PER_TABLE; j++)
			{
				int a = j;
				int b = randomGenerator.nextInt(MicroConstants.RECORDS_PER_TABLE) - 1;
				if(i == 2 || i == 4) //do not touch, this is to link foreign key tuples, etc
					b = a;

				int c = randomGenerator.nextInt(MicroConstants.RECORDS_PER_TABLE) - 1;
				int d = randomGenerator.nextInt(MicroConstants.RECORDS_PER_TABLE) - 1;
				String e = getRandomString(5);

				String statement = "insert into t" + i + " values (" + Integer.toString(a) + "," + Integer.toString(
						b) + "," +
						Integer.toString(c) + "," + Integer.toString(d) + ",'" + e + "')";
				stat.execute(statement);
			}
			connection.commit();
		}
		connection.commit();
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
}
