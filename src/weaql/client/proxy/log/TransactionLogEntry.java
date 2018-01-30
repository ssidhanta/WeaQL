package weaql.client.proxy.log;


/**
 * Created by dnlopes on 09/12/15.
 */
public class TransactionLogEntry
{

	private double selectsTime;
	private double updatesTime;
	private double insertsTime;
	private double deletesTime;
	private double parsingTime;
	private double execTime;
	private double commitTime;
	private double prepareOpTime;
	private double loadFromMainTime;
	private int proxyId;

	public TransactionLogEntry(int proxyId, double selectsTime, double updatesTime, double insertsTime, double
			deletesTime,
							   double parsingTime, double execTime, double commitTime, double prepareOpTime,
							   double loadFromMainTime)
	{
		this.proxyId = proxyId;
		this.selectsTime = selectsTime;
		this.updatesTime = updatesTime;
		this.insertsTime = insertsTime;
		this.deletesTime = deletesTime;
		this.parsingTime = parsingTime;
		this.execTime = execTime;
		this.commitTime = commitTime;
		this.prepareOpTime = prepareOpTime;
		this.loadFromMainTime = loadFromMainTime;
	}

	public String toString()
	{
		StringBuilder buffer = new StringBuilder();
		buffer.append(proxyId).append(",");
		buffer.append(execTime).append(",");
		buffer.append(commitTime).append(",");
		buffer.append(insertsTime).append(",");
		buffer.append(updatesTime).append(",");
		buffer.append(deletesTime).append(",");
		buffer.append(selectsTime).append(",");
		buffer.append(parsingTime).append(",");
		buffer.append(prepareOpTime).append(",");
		buffer.append(loadFromMainTime);

		return buffer.toString();
	}
}
