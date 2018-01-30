package util.csv;


import java.io.*;
import java.util.LinkedList;
import java.util.List;


/**
 * Created by dnlopes on 13/12/15.
 */
public class Metadata
{

	private String jdbc;
	private List<String> clientsList;
	private int numberEmulators;

	public Metadata(String basePath) throws IOException
	{
		clientsList = new LinkedList<>();
		loadMetadata(basePath + File.pathSeparator + basePath);
	}

	private void loadMetadata(String file) throws IOException
	{
		try(BufferedReader br = new BufferedReader(new FileReader(file)))
		{
			String line;
			int lineCount;
			while((line = br.readLine()) != null)
			{

			}
		}

	}
}
