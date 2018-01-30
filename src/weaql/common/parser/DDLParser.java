/*
 * This class defines methods to parse sql schema to create all table and field
 * crdts.
 */

package weaql.common.parser;


import java.io.*;
import java.util.Vector;

import weaql.common.database.constraints.fk.ForeignKeyConstraint;
import weaql.common.database.field.DataField;
import weaql.common.database.util.DatabaseMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import weaql.common.util.RuntimeUtils;
import weaql.common.util.ExitCode;
import weaql.common.database.table.DatabaseTable;


/**
 * The Class SchemaParser.
 */
public class DDLParser
{

	static final Logger LOG = LoggerFactory.getLogger(DDLParser.class);

	private String fileName;
	private DatabaseMetadata databaseMetadata;

	public DDLParser(String fileName)
	{
		this.fileName = fileName;
		this.databaseMetadata = new DatabaseMetadata();
		if(LOG.isTraceEnabled())
			LOG.trace("parser created for annotations file {}", this.fileName);
	}

	public DatabaseMetadata parseAnnotations()
	{
		Vector<String> allTableStrings = this.getAllCreateTableStrings();
                
		for(int i = 0; i < allTableStrings.size(); i++)
		{
                        //System.out.println("****In parseAnnotations parsing table: "+allTableStrings.get(i));
			DatabaseTable table = CreateStatementParser.createTable(this.databaseMetadata,
					allTableStrings.elementAt(i));

			if(table != null)
				this.databaseMetadata.addTable(table);
			else
				RuntimeUtils.throwRunTimeException(
						"cannot create a tableinstance for this table: " + allTableStrings.elementAt(i),
						ExitCode.SCHEMANOCRDTTABLE);
		}

		if(this.databaseMetadata.getAllTables().isEmpty())
			RuntimeUtils.throwRunTimeException("no CRDT tables are created!", ExitCode.SCHEMANOCRDTTABLE);

		this.fillMissingInfo();
		return databaseMetadata;
	}

	private Vector<String> getAllCreateTableStrings()
	{
		BufferedReader br;
		String schemaContentStr = "";
		String line;
		try
		{
			//InputStream is = getClass().getClassLoader().getResourceAsStream(this.fileName);
			//FileReader reader = new FileReader(this.fileName);
			InputStream stream = new FileInputStream(this.fileName);
			br = new BufferedReader(new InputStreamReader(stream, "UTF-8"));
			while((line = br.readLine()) != null)
			{
				schemaContentStr = schemaContentStr + line;
			}
			br.close();
		} catch(IOException e)
		{
			e.printStackTrace();
			System.exit(ExitCode.FILENOTFOUND);
		}

		String[] allStrings = schemaContentStr.split(";");
		Vector<String> allCreateTableStrings = new Vector<>();

		for(int i = 0; i < allStrings.length; i++)
		{
			if(CreateStatementParser.is_Create_Table_Statement(allStrings[i]))
			{
				allCreateTableStrings.add(allStrings[i]);
			}
		}

		if(allCreateTableStrings.isEmpty())
		{
			try
			{
				throw new RuntimeException("This schema doesn't contain any create statement");
			} catch(RuntimeException e)
			{
				System.exit(ExitCode.SCHEMANOCREATSTAT);
			}
		}

		return allCreateTableStrings;
	}

	private void fillMissingInfo()
	{
		// add referenced by fields

		for(DatabaseTable table : databaseMetadata.getAllTables())
		{
			for(DataField field : table.getFieldsList())
			{
				if(field.isForeignKey())
				{
					for(ForeignKeyConstraint fkConstraint : field.getFkConstraints())
					{
						(fkConstraint).getParentTable().setParentTable();
						(fkConstraint).setChildTable(
								(fkConstraint).getFieldsRelations().get(0).getChild().getTable());
						String remoteTableString = (fkConstraint).getParentTable().getName();

						for(String remoteFieldString : (fkConstraint).getParentFields())
						{
							DataField originField = this.databaseMetadata.getTable(remoteTableString).getField(
									remoteFieldString);
							(fkConstraint).addRemoteField(originField);
						}
					}
				}
			}
		}
	}

}
