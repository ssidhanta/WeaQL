package weaql.client.proxy.log;


import java.util.LinkedList;
import java.util.List;
import java.util.Iterator;


/**
 * Created by dnlopes on 09/12/15. Modified by Subhajit October 29.
 */
public class TransactionLog
{

	private final List<TransactionLogEntry> entries;

	public TransactionLog()
	{
		entries = new LinkedList<>();
	}

	public void addEntry(TransactionLogEntry entry)
	{
		entries.add(entry);
	}

	public List<TransactionLogEntry> getEntries()
	{
		return entries;
	}

	public String toString()
	{
		StringBuilder buffer = new StringBuilder();

		//for(TransactionLogEntry entry : entries)
			//buffer.append(entry.toString()).append("\n");
                
                for (Iterator<TransactionLogEntry> iterator = entries.iterator(); iterator.hasNext(); ) {
                        buffer.append(iterator.toString()).append("\n");
                }
		return buffer.toString();
	}

}
