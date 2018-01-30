package applications;

/**
 * Created by dnlopes on 05/06/15.
 */
public interface Workload
{
	public Transaction getNextTransaction(BaseBenchmarkOptions options);
	public float getWriteRate();
	public float getReadRate();
	public float getCoordinatedOperationsRate();

	public void addExtraColumns(StringBuilder buffer);
	public void addExtraColumnValues(StringBuilder buffer);

}
