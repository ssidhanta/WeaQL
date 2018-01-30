package weaql.client.proxy.hook;


import weaql.client.proxy.log.*;
import weaql.client.proxy.Proxy;
import weaql.common.util.Topology;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.LinkedList;
import java.util.List;
import java.util.Iterator;
import java.io.FileWriter;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by dnlopes on 09/12/15.
 */
public class TransactionsLogWritter extends Thread
{

	private List<Proxy> proxies;

	public TransactionsLogWritter()
	{
		proxies = new LinkedList<>();
	}

	public void addProxy(Proxy proxy)
	{
		proxies.add(proxy);
	}

	@Override
	public void run()
	{
		int emulators = Topology.getInstance().getEmulatorsCount();

		String jdbc = System.getProperty("jdbc");
		String emulatorId = System.getProperty("emulatorId");

		StringBuffer buffer = new StringBuffer(Integer.MAX_VALUE);

		buffer.append("proxyid").append(",");
		buffer.append("exec").append(",");
		buffer.append("commit").append(",");
		buffer.append("inserts").append(",");
		buffer.append("updates").append(",");
		buffer.append("deletes").append(",");
		buffer.append("selects").append(",");
		buffer.append("parsing").append(",");
		buffer.append("generate_crdt").append(",");
		buffer.append("load_from_main").append("\n");
                FileWriter out = null;
                String fileName = Topology.getInstance().getReplicatorsCount() + "_replicas_" + proxies.size() * emulators +
					"_users_" + jdbc + "_jdbc_emulator" + emulatorId +
					"_transactions.log";
		try
		{
                out = new FileWriter(fileName);
                for(Proxy proxy : proxies)
		{
			TransactionLog txnLog = proxy.getTransactionLog();
			if(txnLog == null)
				continue;

			for (Iterator<TransactionLogEntry> iterator = txnLog.getEntries().iterator(); iterator.hasNext(); ) 
                            //out.write(buffer.toString() + "\n" + iterator.toString());
                            buffer.append(iterator.toString()).append("\n");
		}
                out.write(buffer.toString());
                out.close();
                } catch(FileNotFoundException e)
                {
			e.printStackTrace();
		} catch (IOException ex) { 
                Logger.getLogger(TransactionsLogWritter.class.getName()).log(Level.SEVERE, null, ex);
            } finally{
                    buffer = null;
                    if(out!=null)
                        out = null;
                }
		//PrintWriter out;
                //try
		//{
			//String fileName = Topology.getInstance().getReplicatorsCount() + "_replicas_" + proxies.size() * emulators +
					//"_users_" + jdbc + "_jdbc_emulator" + emulatorId +
					//"_transactions.log";

			//out = new PrintWriter(fileName);
                        //out = new FileWriter(fileName);
			//out.write(buffer.toString());
			//
		//} catch(FileNotFoundException e)
		//{
			//e.printStackTrace();
		//} 
	}
}
