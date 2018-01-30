package weaql.common.util;


/**
 * Created by dnlopes on 30/03/15.
 */
public final class DatabaseProperties
{

	private final String dbUser;
	private final String dbPwd;
	private final String dbHost;
	private final int dbPort;

	public DatabaseProperties (String user, String pwd, String host, int port)
	{
		this.dbHost = host;
		this.dbPort = port;
		this.dbUser = user;
		this.dbPwd = pwd;
	}

	public String getDbUser()
	{
		return this.dbUser;
	}

	public String getDbPwd()
	{
		return this.dbPwd;
	}

	public String getDbHost()
	{
		return dbHost;
	}

	public int getDbPort()
	{
		return dbPort;
	}

	public String getUrl()
	{
		StringBuilder buffer = new StringBuilder();
		buffer.append(this.getDbHost());
		buffer.append(":");
		buffer.append(this.getDbPort());
		buffer.append("/");

		return buffer.toString();
	}

}
