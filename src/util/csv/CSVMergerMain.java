package util.csv;


import weaql.common.util.ExitCode;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;

import java.io.FileWriter;
import java.io.IOException;
import java.util.*;


/**
 * Created by dnlopes on 10/12/15.
 */
public class CSVMergerMain
{

	private static final String[] HEADER = {"clients", "txn_name", "commits", "aborts", "maxLatency", "minLatency",
			"avgLatency", "avgExecLatency", "avgCommitLatency"};

	public static void main(String args[]) throws IOException
	{
		if(args.length < 6)
		{
			System.err.println(
					"usage: java -jar <jarfile> <basePath> <numberReplicas> <jdbc> " + "<numberEmulators> <outputFile>" +
							" [nClients_1,nClients_2,...,nClients_n]");
			System.exit(ExitCode.WRONG_ARGUMENTS_NUMBER);
		}

		String basePath = args[0];
		String numberReplicas = args[1];
		String jdbc = args[2];
		String numberEmulators = args[3];
		String outputFile = args[4];

		String[] clientsArray = Arrays.copyOfRange(args, 5, args.length);
		List<String> clientsNumber = new ArrayList<>(Arrays.asList(clientsArray));

		CSVMerger merger = new CSVMerger(basePath, Integer.parseInt(numberReplicas), clientsNumber, jdbc,
				Integer.parseInt(numberEmulators));

		merger.merge();
		Map<Integer, Map<String, TrxRecord>> results = merger.getFinals();

		SortedMap<Integer, Map<String, TrxRecord>> sorted = new TreeMap<>();

		for(Map.Entry<Integer, Map<String, TrxRecord>> entry : results.entrySet())
			sorted.put(entry.getKey(), entry.getValue());

		FileWriter fileWriter = new FileWriter(outputFile);
		CSVFormat csvFileFormat = CSVFormat.DEFAULT.withRecordSeparator("\n");

		CSVPrinter csvFilePrinter = new CSVPrinter(fileWriter, csvFileFormat);
		csvFilePrinter.printRecord(HEADER);

		for(Map<String, TrxRecord> entry : sorted.values())
		{
			for(TrxRecord record : entry.values())
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
		}
		fileWriter.flush();
		fileWriter.close();
		csvFilePrinter.close();
	}
}
