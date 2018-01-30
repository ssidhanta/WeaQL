package weaql.common.database.table;


import java.util.*;
import java.util.Map.Entry;

import weaql.common.database.constraints.Constraint;
import weaql.common.database.constraints.check.CheckConstraint;
import weaql.common.database.constraints.fk.ForeignKeyConstraint;
import weaql.common.database.constraints.unique.AutoIncrementConstraint;
import weaql.common.database.constraints.unique.UniqueConstraint;
import weaql.common.database.field.DataField;
import weaql.common.database.util.ExecutionPolicy;
import weaql.common.database.util.PrimaryKey;
import weaql.common.database.field.metadata.DeletedField;
import weaql.common.database.field.metadata.LogicalClockField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import weaql.common.util.RuntimeUtils;
import weaql.common.util.ExitCode;

import weaql.common.util.defaults.DatabaseDefaults;


/**
 * The Class DatabaseTable. Modified by Subhajit on 08/09/17
 */
public class DatabaseTable
{

	private static final Logger LOG = LoggerFactory.getLogger(DatabaseTable.class);

	private ExecutionPolicy executionPolicy;
	private TablePolicy tablePolicy;
	private boolean isParentTable;
	private String name;
	private CRDTTableType tag;
	private boolean hasDeletableParents;
	private boolean containsAutoIncrementField;
	private String primaryKeyString;
	private List<CheckConstraint> checkConstraints;
	private List<UniqueConstraint> uniqueConstraints;
	private List<ForeignKeyConstraint> fkConstraints;
	private List<AutoIncrementConstraint> autoIncrementConstraints;
	private Map<String, AutoIncrementConstraint> autoIncrementConstraintMap;
	private Map<String, Constraint> constraintsMap;
	private PrimaryKey primaryKey;
	private Set<ForeignKeyConstraint> childTablesConstraints;
	private String selectNormalFieldsForQueryString;
	private List<DataField> deltaFields;
	protected DataField contentClockField;
	protected DataField deletedClockField;
	protected DataField deletedField;
	protected int mandatoryFields;
	private boolean freeToInsert;

	protected LinkedHashMap<String, DataField> fieldsMap;
	protected Map<Integer, DataField> sortedFieldsMap;
	protected Map<String, DataField> hiddenFields;
	protected Map<String, DataField> normalFields;

	protected LinkedHashMap<String, DataField> primaryKeyMap;
        
        private Map<ForeignKeyConstraint,ExecutionPolicy> foreignKeyPolicies;

	public DatabaseTable(String name, CRDTTableType tableType, LinkedHashMap<String, DataField> fieldsMap,
						 ExecutionPolicy policy, Map<ForeignKeyConstraint,ExecutionPolicy> foreignKeyPolicies)
	{
		this.mandatoryFields = 0;
		this.executionPolicy = policy;
		this.freeToInsert = true;
		this.hasDeletableParents = false;
		this.fieldsMap = fieldsMap;
		this.name = name;
		this.tag = tableType;
		this.primaryKey = new PrimaryKey();
		this.primaryKeyMap = new LinkedHashMap<>();
		this.childTablesConstraints = new HashSet<>();
		this.sortedFieldsMap = new HashMap<>();
		this.hiddenFields = new HashMap<>();
		this.normalFields = new HashMap<>();
		this.constraintsMap = new HashMap<>();
		this.checkConstraints = new LinkedList<>();
		this.uniqueConstraints = new LinkedList<>();
		this.fkConstraints = new LinkedList<>();
		this.deltaFields = new LinkedList<>();
		this.autoIncrementConstraints = new LinkedList<>();
		this.autoIncrementConstraintMap = new HashMap<>();
		this.containsAutoIncrementField = false;
		this.isParentTable = false;
                
                this.foreignKeyPolicies = new HashMap<>();
                   
                
		if(tableType != CRDTTableType.NONCRDTTABLE)
			this.addHiddenFields();

		this.tablePolicy = new TablePolicy(this.tag);

		for(DataField aDataField : this.fieldsMap.values())
		{
			this.sortedFieldsMap.put(aDataField.getPosition(), aDataField);

			this.uniqueConstraints.addAll(aDataField.getUniqueConstraints());
			this.fkConstraints.addAll(aDataField.getFkConstraints());
			this.checkConstraints.addAll(aDataField.getCheckConstraints());

			if(aDataField.isMetadataField())
				this.hiddenFields.put(aDataField.getFieldName(), aDataField);
			else
			{
				this.normalFields.put(aDataField.getFieldName(), aDataField);
				if(aDataField.getDefaultValue() == null)
					this.mandatoryFields++;
			}

			if(aDataField.isPrimaryKey())
				this.addToPrimaryKey(aDataField);

			if(aDataField.isAutoIncrement())
			{
				this.containsAutoIncrementField = true;
				this.autoIncrementConstraints.add(aDataField.getAutoIncrementConstraint());
				this.autoIncrementConstraintMap.put(aDataField.getFieldName(), aDataField.getAutoIncrementConstraint
						());
			}
			if(aDataField.isDeltaField())
				this.deltaFields.add(aDataField);
		}

		setPrimaryKeyString(assemblePrimaryKeyString());
		generateSelectFieldsForQuery();

		for(UniqueConstraint c : this.uniqueConstraints)
		{
			/*
			if(!tablePolicy.allowInserts() && !tablePolicy.allowUpdates())
				c.setRequiresCoordination(false);
                      */

			if(!tablePolicy.allowInserts() && c.isPrimaryKey())
				c.setRequiresCoordination(false);

			this.constraintsMap.put(c.getConstraintIdentifier(), c);

			if(c.requiresCoordination())
				freeToInsert = false;
		}

		for(Constraint c : this.fkConstraints)
		{
			if(((ForeignKeyConstraint) c).getParentTable().getTablePolicy().allowDeletes())
				this.hasDeletableParents = true;

			this.constraintsMap.put(c.getConstraintIdentifier(), c);
		}
		for(Constraint c : this.checkConstraints)
			this.constraintsMap.put(c.getConstraintIdentifier(), c);
	}
        
        public PrimaryKey getPrimaryKey()
	{
		return this.primaryKey;
	}
        
	public Map<ForeignKeyConstraint,ExecutionPolicy> getForeignKeyPolicies()
	{
		return this.foreignKeyPolicies;
	}
        
        public void setForeignKeyPolicies(Map<ForeignKeyConstraint,ExecutionPolicy> foreignKeyPolicies)
	{
		this.foreignKeyPolicies = foreignKeyPolicies;
	}

	public void addToPrimaryKey(DataField field)
	{
		if(this.primaryKeyMap.containsKey(field.getFieldName()))
			RuntimeUtils.throwRunTimeException("duplicated field for primary key", ExitCode.DUPLICATED_FIELD);

		this.primaryKeyMap.put(field.getFieldName(), field);
		this.primaryKey.addField(field);

		if(field.isAutoIncrement())
			this.containsAutoIncrementField = true;
	}

	public String getName()
	{
		return this.name;
	}

	public CRDTTableType getTableType()
	{
		return this.tag;
	}

	public Map<String, DataField> getFieldsMap()
	{
		return this.fieldsMap;
	}

	public List<DataField> getFieldsList()
	{
		Collection<DataField> dataFields = this.fieldsMap.values();
		if(dataFields instanceof List)
		{
			return (List<DataField>) dataFields;
		} else
		{
			return new ArrayList<>(dataFields);
		}
	}

	public HashMap<String, DataField> getPrimaryKeysMap()
	{
		return this.primaryKeyMap;
	}

	public Set<String> getPrimaryKeysNamesList()
	{
		return this.primaryKeyMap.keySet();
	}

	public int getFieldCount()
	{
		int tempCount = fieldsMap.size() - this.getNumOfHiddenFields();
		if(tempCount <= 0)
		{
			throw new RuntimeException("You have zero or negative number of data fields");
		}
		return tempCount;
	}

	public DataField getField(String name)
	{

		if(this.fieldsMap == null)
		{
			try
			{
				throw new RuntimeException("data field map has not been initialized!");
			} catch(RuntimeException e)
			{
				System.exit(ExitCode.NOINITIALIZATION);
			}
		}

		if(!fieldsMap.containsKey(name))
		{
			try
			{
				throw new RuntimeException("record is not found " + name + "!");
			} catch(RuntimeException e)
			{
				System.exit(ExitCode.HASHMAPNOEXIST);
			}
		}

		return fieldsMap.get(name);

	}

	public DataField getField(int index)
	{

		if(this.sortedFieldsMap == null)
		{
			try
			{
				throw new RuntimeException("data field map has not been initialized!");
			} catch(RuntimeException e)
			{
				System.exit(ExitCode.NOINITIALIZATION);
			}
		}

		if(this.sortedFieldsMap.size() <= index)
		{
			try
			{
				throw new RuntimeException("data field index beyond the size of data file map!");
			} catch(RuntimeException e)
			{
				System.exit(ExitCode.OUTOFRANGE);
			}
		}

		if(!this.sortedFieldsMap.containsKey(index))
		{
			try
			{
				throw new RuntimeException("record is not found for " + index + " index!");
			} catch(RuntimeException e)
			{
				System.exit(ExitCode.HASHMAPNOEXIST);
			}
		}

		return this.sortedFieldsMap.get(index);

	}

	public Set<String> findMisingDataField(List<String> colList, List<String> valueList)
	{
		if(getFieldCount() == colList.size() || getFieldCount() == valueList.size())
		{
			if(LOG.isTraceEnabled())
				LOG.trace("no missing fields");
			return null;
		}
		if(colList.size() > 0)
		{
			assert (valueList.size() == colList.size());
			Set<String> dfNameSet = new HashSet<>();
			Set<String> colSet = new HashSet<>(colList);
			int i = 0;
			Iterator<Entry<Integer, DataField>> it = sortedFieldsMap.entrySet().iterator();

			while(it.hasNext() && i < getFieldCount())
			{
				Entry<Integer, DataField> en = it.next();
				if(!colSet.contains(en.getValue().getFieldName()))
				{
					dfNameSet.add(en.getValue().getFieldName());
				}
				i++;
			}

			return dfNameSet;
		} else
		{
			assert (getFieldCount() == valueList.size());
			return null;
		}
	}

	public boolean isPrimaryKeyMissingFromWhereClause(String whereClauseStr)
	{
		HashMap<String, DataField> primaryKeysMap = this.getPrimaryKeysMap();

		for(Entry<String, DataField> stringDataFieldEntry : primaryKeysMap.entrySet())
		{
			DataField pk = stringDataFieldEntry.getValue();
			if(!whereClauseStr.contains(pk.getFieldName()))
				return true;
		}
		return false;
	}

	public String generatedPrimaryKeyQuery(String whereClauseStr)
	{
		StringBuilder selectStr = new StringBuilder("select ");
		selectStr.append(this.getPrimaryKeyString());
		selectStr.append(" from ");
		selectStr.append(this.getName());
		selectStr.append(" ");
		selectStr.append(" where ");
		selectStr.append(whereClauseStr);
		return selectStr.toString();
	}

	public int getNumOfHiddenFields()
	{
		return this.hiddenFields.size();
	}

	public void setPrimaryKeyString(String primaryKeyString)
	{
		this.primaryKeyString = primaryKeyString;
	}

	public String getPrimaryKeyString()
	{
		if(this.primaryKeyString.equals(""))
			setPrimaryKeyString(assemblePrimaryKeyString());

		return this.primaryKeyString;
	}

	public Constraint getConstraint(String constraintId)
	{
		return this.constraintsMap.get(constraintId);
	}

	public ExecutionPolicy getExecutionPolicy()
	{
		return this.executionPolicy;
	}

	public Map<String, DataField> getNormalFields()
	{
		return this.normalFields;
	}

	public void setParentTable()
	{
		this.isParentTable = true;
	}

	public boolean isParentTable()
	{
		return this.isParentTable;
	}

	public void addChildTableConstraint(ForeignKeyConstraint fkConstraint)
	{
		this.childTablesConstraints.add(fkConstraint);
	}

	private void generateSelectFieldsForQuery()
	{
		StringBuilder buffer = new StringBuilder();

		Iterator<DataField> fieldsIt = this.normalFields.values().iterator();

		while(fieldsIt.hasNext())
		{
			buffer.append(fieldsIt.next().getFieldName());
			if(fieldsIt.hasNext())
				buffer.append(",");
		}

		this.selectNormalFieldsForQueryString = buffer.toString();
	}

	public String getNormalFieldsSelection()
	{
		return this.selectNormalFieldsForQueryString;
	}

	public TablePolicy getTablePolicy()
	{
		return this.tablePolicy;
	}

	public AutoIncrementConstraint getAutoIncrementConstraint(String fieldName)
	{
		return this.autoIncrementConstraintMap.get(fieldName);
	}

	public List<CheckConstraint> getCheckConstraints()
	{
		return this.checkConstraints;
	}

	public List<UniqueConstraint> getUniqueConstraints()
	{
		return this.uniqueConstraints;
	}

	public List<ForeignKeyConstraint> getFkConstraints()
	{
		return this.fkConstraints;
	}

	public List<AutoIncrementConstraint> getAutoIncrementConstraints()
	{
		return this.autoIncrementConstraints;
	}

	public List<DataField> getDeltaFields()
	{
		return this.deltaFields;
	}

	public boolean isFreeToInsert()
	{
		return freeToInsert;
	}

	private String assemblePrimaryKeyString()
	{
		StringBuilder pkStrBuilder = new StringBuilder("");
		Iterator<String> it = this.getPrimaryKeysNamesList().iterator();
		int index = 0;

		while(it.hasNext())
		{
			String singlePkStr = it.next();
                        //System.out.println("***in Databasetable singlePkStr:="+singlePkStr);
			if(index == 0)
			{
				pkStrBuilder.append(singlePkStr);
			} else
			{
				pkStrBuilder.append(",");
				pkStrBuilder.append(singlePkStr);
			}
			index++;
		}

		return pkStrBuilder.toString();
	}

	private void addHiddenFields()
	{
		DataField deletedField = new DeletedField(this.name, fieldsMap.size());
		this.fieldsMap.put(deletedField.getFieldName(), deletedField);
		this.deletedField = deletedField;
		this.deletedField.setDefaultValue("1");
		this.hiddenFields.put(deletedField.getFieldName(), deletedField);

		DataField contentClock = new LogicalClockField(this.name, fieldsMap.size(),
				DatabaseDefaults.CONTENT_CLOCK_COLUMN);
		this.fieldsMap.put(contentClock.getFieldName(), contentClock);
		this.hiddenFields.put(contentClock.getFieldName(), contentClock);

		DataField deletedClock = new LogicalClockField(this.name, fieldsMap.size(),
				DatabaseDefaults.DELETED_CLOCK_COLUMN);
		this.fieldsMap.put(deletedClock.getFieldName(), deletedClock);
		this.hiddenFields.put(deletedClock.getFieldName(), deletedClock);

		this.contentClockField = contentClock;
		this.deletedClockField = deletedClock;

	}

	public boolean hasDeletableParents()
	{
		return hasDeletableParents;
	}

	public String toString()
	{
		String myString = "TableName: " + this.name + " \n";
		myString += "primary key maps --> \n";

		for(Entry<String, DataField> entry : this.primaryKeyMap.entrySet())
		{
			myString += entry.getValue().toString() + " \n ";
		}

		myString += "data field maps -------->\n";

		for(Entry<String, DataField> entry : fieldsMap.entrySet())
		{
			myString += entry.getValue().toString() + " \n ";
		}

		myString += " is contained AutoIncremental fields: " + this.containsAutoIncrementField + "\n";

		return myString;

	}

	public boolean isChildTable()
	{
		return this.fkConstraints.size() != 0;
	}

	public int getMandatoryFields()
	{
		return mandatoryFields;
	}
}
