package util.csv;


import weaql.common.util.ExitCode;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;

import java.io.*;
import java.util.*;


/**
 * Created by dnlopes on 14/12/15.
 */
public class CSVRunsMerger
{

	private static final String[] HEADER = {"clients", "txn_name", "commits", "aborts", "maxLatency", "minLatency",
			"avgLatency", "avgExecLatency", "avgCommitLatency"};

	private String basePath;
	private int runs;
	private List<String> files;
	private Map<String, List<TrxRecord>> trxRecordsMap;

	private Map<String, TrxRecord> finalResults;

	public CSVRunsMerger(String basePath, int runs)
	{
		this.basePath = basePath;
		this.runs = runs;
		this.files = new ArrayList<>();
		this.trxRecordsMap = new HashMap<>();
		this.finalResults = new HashMap<>();

		for(int i = 1; i <= runs; i++)
			files.add("run" + i + ".csv");
	}

	public void merge() throws IOException
	{
		for(String aFile : files)
		{
			Reader in = new FileReader(basePath + "/" + aFile);
			Iterable<CSVRecord> records = CSVFormat.EXCEL.withIgnoreEmptyLines().withSkipHeaderRecord().withHeader(
					HEADER).parse(in);

			for(CSVRecord record : records)
			{
				String clients = record.get("clients");
				String trxName = record.get("txn_name");
				int commits = Integer.parseInt(record.get("commits"));
				int aborts = Integer.parseInt(record.get("aborts"));
				double maxLatency = Double.parseDouble(record.get("maxLatency"));
				double minLatency = Double.parseDouble(record.get("minLatency"));
				double avgLatency = Double.parseDouble(record.get("avgLatency"));
				double avgExecLatency = Double.parseDouble(record.get("avgExecLatency"));
				double avgCommitLatency = Double.parseDouble(record.get("avgCommitLatency"));

				TrxRecord trxRecord = new TrxRecord(Integer.parseInt(clients), trxName, commits, aborts, maxLatency,
						minLatency, avgLatency, avgExecLatency, avgCommitLatency);

				String key = clients + "_" + trxName;
				if(!trxRecordsMap.containsKey(key))
					trxRecordsMap.put(key, new LinkedList<TrxRecord>());
				trxRecordsMap.get(key).add(trxRecord);

			}
		}

		for(List<TrxRecord> partials : trxRecordsMap.values())
			mergeList(partials);
	}

	private void mergeList(List<TrxRecord> aList)
	{
		int avgCommit = 0;
		int avgAborts = 0;
		double avgMaxLatency = 0;
		double avgMinLatency = 0;
		double avgLatency = 0;
		double avgExecLatency = 0;
		double avgCommitLatency = 0;
		String name = aList.get(0).getName();
		int clients = aList.get(0).getClients();

		for(TrxRecord record : aList)
		{
			avgCommit += record.getCommits();
			avgAborts += record.getAborts();
			avgLatency += record.getAvgLatency();
			avgMaxLatency += record.getMaxLatency();
			avgMinLatency += record.getMinLatency();
			avgExecLatency += record.getAvgExecLatency();
			avgCommitLatency += record.getAvgCommitLatency();
		}

		avgCommit = avgCommit / runs;
		avgAborts = avgAborts / runs;
		avgLatency = avgLatency / runs;
		avgMaxLatency = avgMaxLatency / runs;
		avgMinLatency = avgMinLatency / runs;
		avgExecLatency = avgExecLatency / runs;
		avgCommitLatency = avgCommitLatency / runs;

		TrxRecord finalRecord = new TrxRecord(clients, name, avgCommit, avgAborts, avgMaxLatency, avgMinLatency,
				avgLatency, avgExecLatency, avgCommitLatency);
		finalResults.put(finalRecord.getClients() + "_" + finalRecord.getName(), finalRecord);

	}

	public Map<String, TrxRecord> getFinalResults()
	{
		return finalResults;
	}

	public static void main(String args[]) throws IOException
	{
		if(args.length < 3)
		{
			System.err.println("usage: java -jar <jarfile> <basePath> <numberOfRuns> <outputFile>");
			System.exit(ExitCode.WRONG_ARGUMENTS_NUMBER);
		}

		Comparator<String> comparator = new Comparator<String>()
		{
			@Override
			public int compare(String o1, String o2)
			{
				String[] splitted = o1.split("_");
				String[] splitted2 = o2.split("_");

				int c1 = Integer.parseInt(splitted[0]);
				int c2 = Integer.parseInt(splitted2[0]);

				if(c1 < c2)
					return -1;
				else if ( c1 > c2)
					return 1;
				else
				{
					return splitted[1].compareTo(splitted2[1]);
				}
			}
		};

		String basePath = args[0];
		int runs = Integer.parseInt(args[1]);
		String outputFile = args[2];

		CSVRunsMerger merger = new CSVRunsMerger(basePath, runs);

		merger.merge();

		Map<String, TrxRecord> finalRecords = merger.getFinalResults();

		SortedMap<String, TrxRecord> sorted = new TreeMap<>(comparator);

		for(Map.Entry<String, TrxRecord> entry : finalRecords.entrySet())
			sorted.put(entry.getKey(), entry.getValue());

		FileWriter fileWriter = new FileWriter(outputFile);
		CSVFormat csvFileFormat = CSVFormat.DEFAULT.withRecordSeparator("\n");

		CSVPrinter csvFilePrinter = new CSVPrinter(fileWriter, csvFileFormat);
		csvFilePrinter.printRecord(HEADER);

		for(TrxRecord record : sorted.values())
		{
			List csvRecord = new ArrayList();
			csvRecord.add(record.getClients());
			csvRecord.add(record.getName());
			csvRecord.add(record.getCommits());
			csvRecord.add(record.getAborts());
			csvRecord.add(record.getMaxLatency());
			csvRecord.add(record.getMinLatency());
			csvRecord.add(record.getAvgLatency());
			csvRecord.add(record.getAvgExecLatency());
			csvRecord.add(record.getAvgCommitLatency());
			csvFilePrinter.printRecord(csvRecord);
		}
		fileWriter.flush();
		fileWriter.close();
		csvFilePrinter.close();
	}


}
