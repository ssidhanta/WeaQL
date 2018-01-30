package util.csv;


import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;


/**
 * Created by dnlopes on 10/12/15.
 */
public class CSVMerger
{

	private static final Logger LOG = LoggerFactory.getLogger(CSVMerger.class);

	public static final String IGNORED = "txns";
	private static final String[] HEADER = {"txn_name", "commits", "aborts", "maxLatency", "minLatency", "avgLatency",
			"avgExecLatency", "avgCommitLatency"};

	private String jdbc;
	private Map<Integer, Map<String, TrxRecord>> finals;

	private Map<Integer, List<String>> filesByUsersNumber;
	private String basePath;
	private List<String> clientsList;
	private int replicasNumber;
	private int numberEmulators;

	public CSVMerger(String basePath, int numberReplicas, List<String> clientsList, String jdbc, int numberEmulators)
	{
		String sufix = "_" + jdbc + "_jdbc_emulator";

		this.numberEmulators = numberEmulators;
		this.jdbc = jdbc;
		this.basePath = basePath;
		this.clientsList = clientsList;
		this.replicasNumber = numberReplicas;
		this.filesByUsersNumber = new HashMap<>();
		this.finals = new HashMap<>();

		String prefix = replicasNumber + "_replicas_";

		for(String clients : this.clientsList)
		{
			List<String> aList = new ArrayList<>();

			filesByUsersNumber.put(Integer.parseInt(clients), aList);
			String aPrefix = prefix + clients + "_users" + sufix;

			for(int i = 1; i <= numberEmulators; i++)
			{
				String aFile = aPrefix + i + ".csv";
				aList.add(aFile);
			}
		}
	}

	public void merge() throws IOException
	{
		for(Map.Entry<Integer, List<String>> entry : filesByUsersNumber.entrySet())
		{
			int clients = entry.getKey();
			LOG.info("merging files from {} clients", clients);
			mergeFiles(clients, entry.getValue());
		}
	}

	public Map<Integer, Map<String, TrxRecord>> getFinals()
	{
		return finals;
	}

	private void mergeFiles(int clientsNumber, List<String> emulatorsListFiles) throws IOException
	{
		List<CSVRecord> relevantRecords = new LinkedList<>();

		for(String aFile : emulatorsListFiles)
		{
			Reader in = new FileReader(basePath + "/" + aFile);
			Iterable<CSVRecord> records = CSVFormat.EXCEL.withIgnoreEmptyLines().withSkipHeaderRecord().withHeader(
					HEADER).parse(in);

			for(CSVRecord record : records)
			{
				String firstName = record.get("txn_name");
				if(firstName.contains("ms"))
					continue;
				else
					relevantRecords.add(record);
			}
		}

		//mergePartialEmulatorsFiles(clientsNumber, relevantRecords)
		mergeRecords(clientsNumber, relevantRecords);
	}

	private void mergeRecords(int clientsNumber, List<CSVRecord> records)
	{
		Map<String, List<TrxRecord>> partials = new HashMap<>();
		Map<String, TrxRecord> finalResults = new HashMap<>();
		for(CSVRecord record : records)
		{
			String trxName = record.get("txn_name");
			int commits = Integer.parseInt(record.get("commits"));

			if(commits == 0)
				continue;

			int aborts = Integer.parseInt(record.get("aborts"));
			double maxLatency = Double.parseDouble(record.get("maxLatency"));
			double minLatency = Double.parseDouble(record.get("minLatency"));
			double avgLatency = Double.parseDouble(record.get("avgLatency"));
			double avgExecLatency = Double.parseDouble(record.get("avgExecLatency"));
			double avgCommitLatency = Double.parseDouble(record.get("avgCommitLatency"));

			TrxRecord trxRecord = new TrxRecord(clientsNumber, trxName, commits, aborts, maxLatency, minLatency,
					avgLatency, avgExecLatency, avgCommitLatency);

			if(!partials.containsKey(trxRecord.getName()))
				partials.put(trxRecord.getName(), new LinkedList<TrxRecord>());

			partials.get(trxRecord.getName()).add(trxRecord);
		}

		for(List<TrxRecord> partial : partials.values())
		{
			TrxRecord mergedTrxRecord = mergePartialsTrxValues(partial, clientsNumber);
			finalResults.put(mergedTrxRecord.getName(), mergedTrxRecord);
		}

		List aList = new ArrayList(finalResults.values());

		TrxRecord allTrx = mergePartialsTrxValues(aList, clientsNumber);
		allTrx.setName("ALL");

		finalResults.put(allTrx.getName(), allTrx);

		finals.put(clientsNumber, finalResults);
	}

	private TrxRecord mergePartialsTrxValues(List<TrxRecord> trxRecords, int clients)
	{
		String name = "null";
		int commits = 0;
		int aborts = 0;
		double maxLatency, minLatency, avgLatency, avgExecLatency, avgCommitLatency = 0;

		avgLatency = avgCommitLatency = avgExecLatency = maxLatency = 0;
		minLatency = Double.MAX_VALUE;

		for(TrxRecord record : trxRecords)
		{
			int myCommit = record.getCommits();
			name = record.getName();
			commits += record.getCommits();
			aborts += record.getAborts();
			avgLatency += myCommit * record.getAvgLatency();
			avgCommitLatency += myCommit * record.getAvgCommitLatency();
			avgExecLatency += myCommit * record.getAvgExecLatency();

			if(record.getMaxLatency() > maxLatency)
				maxLatency = record.getMaxLatency();
			if(record.getMinLatency() < minLatency)
				minLatency = record.getMinLatency();
		}

		avgCommitLatency = avgCommitLatency / commits;
		avgExecLatency = avgExecLatency / commits;
		avgLatency = avgLatency / commits;

		return new TrxRecord(clients, name, commits, aborts, maxLatency, minLatency, avgLatency, avgExecLatency,
				avgCommitLatency);

	}

}
