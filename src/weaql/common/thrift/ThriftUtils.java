package weaql.common.thrift;


import weaql.client.execution.TransactionContext;
import weaql.common.database.constraints.unique.AutoIncrementConstraint;
import weaql.common.database.field.DataField;
import weaql.common.database.table.DatabaseTable;
import weaql.common.database.util.SemanticPolicy;
import org.apache.thrift.TBase;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Created by dnlopes on 16/07/15.
 */
public class ThriftUtils
{

	private static final Logger LOG = LoggerFactory.getLogger(ThriftUtils.class);

	public static CoordinatorRequest decodeCoordinatorRequest(byte[] requestByteArray)
	{
		TDeserializer deserializer = new TDeserializer(new TBinaryProtocol.Factory());
		CoordinatorRequest req = new CoordinatorRequest();
		try
		{
			deserializer.deserialize(req, requestByteArray);
			return req;
		} catch(TException e)
		{
			return null;
		}
	}

	public static byte[] encodeThriftObject(TBase request)
	{
		TSerializer serializer = new TSerializer(new TBinaryProtocol.Factory());
		try
		{
			return serializer.serialize(request);
		} catch(TException e)
		{
			return null;
		}
	}

	public static void createSymbolEntry(TransactionContext context, String symbol, DataField dField, DatabaseTable
			table)
	{
		SymbolEntry symbolEntry = new SymbolEntry();
		symbolEntry.setSymbol(symbol);
		symbolEntry.setTableName(dField.getTableName());
		symbolEntry.setFieldName(dField.getFieldName());
		symbolEntry.setRequiresCoordination(false);

		context.getPreCompiledTxn().putToSymbolsMap(symbol, symbolEntry);

		if(!dField.isAutoIncrement())
			LOG.warn("field with semantic value must either be auto_increment or " + "given by the " +
					"application" +
					" " +
					"level");

		if(dField.getSemantic() == SemanticPolicy.SEMANTIC)
		{
			symbolEntry.setRequiresCoordination(true);

			AutoIncrementConstraint autoIncrementConstraint = table.getAutoIncrementConstraint(
					dField.getFieldName());
			RequestValue request = new RequestValue();

			request.setConstraintId(autoIncrementConstraint.getConstraintIdentifier());
			request.setTempSymbol(symbol);
			request.setFieldName(dField.getFieldName());

			context.getCoordinatorRequest().addToRequests(request);
		}
	}
}