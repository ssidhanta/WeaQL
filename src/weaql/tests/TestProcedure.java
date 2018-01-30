package weaql.tests;


import applications.micro.MicroDatabase;
import weaql.common.util.ConnectionFactory;
import org.apache.commons.lang3.time.StopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import weaql.common.util.DatabaseProperties;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;


/**
 * Created by dnlopes on 05/05/15.
 */
public class TestProcedure
{

	static final Logger LOG = LoggerFactory.getLogger(TestProcedure.class);

	private final static String DB_HOST = "172.16.24.184";

	public static void main(String args[]) throws SQLException, ClassNotFoundException
	{
		DatabaseProperties props = new DatabaseProperties("sa", "101010", DB_HOST, 3306);

		MicroDatabase microDatabase = new MicroDatabase(props);
		microDatabase.setupDatabase(true);

		Connection conn = ConnectionFactory.getDefaultConnection(props, "micro");
		conn.setAutoCommit(false);
		Statement stat = conn.createStatement();

		//deleteCascade(conn, stat);
		//microDatabase.setupDatabase(false);
		updateDelColumn(conn, stat);
		//testFunctionPerformance(stat);
	}


	private static void deleteCascade(Connection conn, Statement stat) throws SQLException
	{

		StopWatch watch = new StopWatch();
		watch.start();
		stat.execute("delete from t0 where a=0");
		watch.stop();
		LOG.info("delete on cascade time elapsed: {} ms", watch.getTime());
		conn.rollback();
	}

	private static void updateDelColumn(Connection conn, Statement stat) throws SQLException
	{

		StopWatch watch = new StopWatch();
		watch.start();
		stat.execute("update t0 set _del=1 where a=0");
		stat.execute("update t1 set _del=1 where a=0 AND testClock('0-0', '0-0') = 4");
		//stat.execute("update t1 set _del=1 where a=0 AND testClock('0-0', '0-0') = 4");
		watch.stop();
		LOG.info("update _SP_del \"on cascade\" time elapsed: {} ms", watch.getTime());
		conn.rollback();
	}

	private static void testFunctionPerformance(Statement stat) throws SQLException
	{
		StopWatch watch = new StopWatch();
		watch.start();
		for(int i = 0; i < 1000; i++)
			//stat.execute("select testClock('2-1-1', '1-2-1')");
			stat.execute("select testClock('3-2-1-1-1-1', '2-3-1-1-1-1')");

		watch.stop();
		LOG.info("testClock runtime: {} ms", watch.getTime());
	}
}
