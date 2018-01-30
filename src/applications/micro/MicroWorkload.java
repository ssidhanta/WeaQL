package applications.micro;


import applications.BaseBenchmarkOptions;
import applications.Transaction;
import applications.Workload;


/**
 * Created by dnlopes on 05/06/15.
 */
public class MicroWorkload implements Workload
{

	public MicroWorkload()
	{
	}

	@Override
	public Transaction getNextTransaction(BaseBenchmarkOptions options)
	{
		return null;
	}

	@Override
	public float getWriteRate()
	{
		return 0;
	}

	@Override
	public float getReadRate()
	{
		return 0;
	}

	@Override
	public float getCoordinatedOperationsRate()
	{
		return 0;
	}

	@Override
	public void addExtraColumns(StringBuilder buffer)
	{

	}

	@Override
	public void addExtraColumnValues(StringBuilder buffer)
	{

	}
}
