package weaql.common.util;

/**
 * Created by dnlopes on 21/03/15.
 */
public class RuntimeUtils
{

	public static void throwRunTimeException(String message, int exitCode)
	{
		try
		{
			throw new RuntimeException(message);
		} catch(RuntimeException e)
		{
			e.printStackTrace();
			System.exit(exitCode);
		}
	}

	public static void throwRunTimeException(Exception e)

	{
		try
		{
			throw e;
		} catch(Exception e1)
		{
			e1.printStackTrace();
			System.exit(1);
		}
	}

}
