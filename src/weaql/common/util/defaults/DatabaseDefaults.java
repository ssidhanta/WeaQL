package weaql.common.util.defaults;

/**
 * Created by dnlopes on 05/03/15.
 */
public interface DatabaseDefaults
{
	String CONTENT_CLOCK_COLUMN = "_cclock";
	String DELETED_CLOCK_COLUMN = "_dclock";
	String DELETED_COLUMN = "_del";
	String NOT_DELETED_VALUE = "0";
	String DELETED_VALUE = "1";
	String DEFAULT_URL_PREFIX = "jdbc:mysql://";
	String SQLITE_URL_PREFIX = "jdbc:sqlite:";
	String DEFAULT_PASSWORD= "101010";
	String DEFAULT_USER = "sa";
	int DEFAULT_MYSQL_PORT = 3306;
}