package applications.tpcc;


import applications.BaseBenchmarkOptions;
import applications.GeneratorUtils;
import applications.Transaction;
import applications.Workload;
import applications.tpcc.txn.*;


/**
 * Created by dnlopes on 05/09/15.
 */
public class TpccWorkload implements Workload
{

	@Override
	public Transaction getNextTransaction(BaseBenchmarkOptions options)
	{
		int random = GeneratorUtils.randomNumberIncludeBoundaries(1, 100);

		if(random <= TpccConstants.DELIVERY_TXN_RATE)
			return new DeliveryTransaction(options);
		else if(random <= TpccConstants.DELIVERY_TXN_RATE + TpccConstants.NEW_ORDER_TXN_RATE)
			return new NewOrderTransaction(options);
		else if(random <= TpccConstants.DELIVERY_TXN_RATE + TpccConstants.NEW_ORDER_TXN_RATE + TpccConstants
				.ORDER_STAT_TXN_RATE)
			return new OrderStatTransaction(options);
		else if(random <= TpccConstants.DELIVERY_TXN_RATE + TpccConstants.NEW_ORDER_TXN_RATE + TpccConstants
				.ORDER_STAT_TXN_RATE + TpccConstants.PAYMENT_TXN_RATE)
			return new PaymentTransaction(options);
		else
			return new StockLevelTransaction(options);
	}

	@Override
	public float getWriteRate()
	{
		return (TpccConstants.NEW_ORDER_TXN_RATE + TpccConstants.DELIVERY_TXN_RATE + TpccConstants.PAYMENT_TXN_RATE) /
				100f;
	}

	@Override
	public float getReadRate()
	{
		return (TpccConstants.ORDER_STAT_TXN_RATE + TpccConstants.STOCK_LEVEL_TXN_RATE) / 100;
	}

	@Override
	public float getCoordinatedOperationsRate()
	{
		return 0;
	}

	@Override
	public void addExtraColumns(StringBuilder buffer)
	{
		buffer.append(",neworder_rate,orderstat_rate,payment_rate,delivery_rate,stocklevel_rate");
	}

	@Override
	public void addExtraColumnValues(StringBuilder buffer)
	{
		buffer.append(",");
		buffer.append(TpccConstants.NEW_ORDER_TXN_RATE);
		buffer.append(",");
		buffer.append(TpccConstants.ORDER_STAT_TXN_RATE);
		buffer.append(",");
		buffer.append(TpccConstants.PAYMENT_TXN_RATE);
		buffer.append(",");
		buffer.append(TpccConstants.DELIVERY_TXN_RATE);
		buffer.append(",");
		buffer.append(TpccConstants.STOCK_LEVEL_TXN_RATE);
	}
}
