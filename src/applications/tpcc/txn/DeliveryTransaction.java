package applications.tpcc.txn;


import applications.AbstractTransaction;
import applications.BaseBenchmarkOptions;
import applications.GeneratorUtils;
import applications.Transaction;
import applications.tpcc.TpccConstants;
import applications.tpcc.TpccStatements;
import org.apache.commons.dbutils.DbUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.Calendar;
import java.util.Date;
//import weaql.client.proxy.SandboxExecutionProxy;


/**
 * Created by dnlopes on 05/09/15. Modified by Subhajit Sidhanta 30/08/2017
 */
public class DeliveryTransaction extends AbstractTransaction implements Transaction
{

	private static final Logger logger = LoggerFactory.getLogger(NewOrderTransaction.class);

	private final BaseBenchmarkOptions options;
	private DeliveryMetadata metadata;

	public DeliveryTransaction(BaseBenchmarkOptions options)
	{
		super(TpccConstants.DELIVERY_TXN_NAME);

		this.options = options;
	}

	@Override
	public boolean executeTransaction(Connection con)//, SandboxExecutionProxy proxy)
	{
		this.metadata = createDeliveryMetadata();

		if(this.metadata == null)
		{
			logger.error("failed to generate txn metadata");
			return false;
		}

		try
		{
			con.setReadOnly(false);
		} catch(SQLException e)
		{
			lastError = e.getMessage();
			this.rollbackQuietly(con);
			return false;
		}

		TpccStatements statements = TpccStatements.getInstance();
		PreparedStatement ps = null;
		ResultSet rs = null;

		int w_id = this.metadata.getWarehouseId();
		int o_carrier_id = this.metadata.getCarrierId();
		int d_id = 0;
		int c_id = 0;
		int no_o_id = 0;
		float ol_total = 0;

		Calendar calendar = Calendar.getInstance();
		Date now = calendar.getTime();
		Timestamp currentTimeStamp = new Timestamp(now.getTime());
                
                java.text.SimpleDateFormat sdf = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                
		String op = null;
		for(d_id = 1; d_id <= TpccConstants.DISTRICTS_PER_WAREHOUSE; d_id++)
		{
			op = "SELECT COALESCE(MIN(no_o_id),0) FROM new_orders WHERE no_d_id = " + d_id + " AND " +
						"no_w_id" +
						" " +
						"= " + w_id;
			if(logger.isTraceEnabled())
				logger.trace("SELECT COALESCE(MIN(no_o_id),0) FROM new_orders WHERE no_d_id = " + d_id + " AND " +
						"no_w_id" +
						" " +
						"= " + w_id);
			try
			{
				ps = statements.createPreparedStatement(con, 25);
				ps.setInt(1, d_id);
				ps.setInt(2, w_id);
                                //System.out.println("*** DeliveryTransaction proxy:="+proxy);
				//rs = proxy.executeQuery(op); 
                                rs = ps.executeQuery();

				if(rs!=null && rs.next())
				{
					no_o_id = rs.getInt(1);

					if(this.options.isCRDTDriver()) // small hack
					{
						if(rs.next())
						{
							no_o_id = rs.getInt(1);
						}
					}
				}

				if(rs!=null)
                                    rs.close();
			} catch(SQLException e)
			{
				lastError = e.getMessage();
				DbUtils.closeQuietly(rs);
				DbUtils.closeQuietly(ps);
				this.rollbackQuietly(con);
				return false;
			}

			if(no_o_id == 0)
				continue;
			else
			{
				if(logger.isDebugEnabled())
					logger.debug("No_o_id did not equal 0 -> " + no_o_id);
			}
			op = "DELETE FROM new_orders WHERE no_o_id = " + no_o_id + " AND no_d_id = " + d_id + " AND " +
						"no_w_id = " + w_id;
			if(logger.isTraceEnabled())
				logger.trace("DELETE FROM new_orders WHERE no_o_id = " + no_o_id + " AND no_d_id = " + d_id + " AND " +
						"no_w_id = " + w_id);
			try
			{
				ps = statements.createPreparedStatement(con, 26);
				ps.setInt(1, no_o_id);
				ps.setInt(2, d_id);
				ps.setInt(3, w_id);
				
                                ps.executeUpdate();
				//int ups = proxy.executeUpdate(op);	

			} catch(SQLException e)
			{
				lastError = e.getMessage();
				DbUtils.closeQuietly(rs);
				DbUtils.closeQuietly(ps);
				this.rollbackQuietly(con);
				return false;
			}
			op = "SELECT o_c_id FROM orders WHERE o_id = " + no_o_id + " AND o_d_id = " + d_id + " AND " +
						"o_w_id = " + w_id;
			if(logger.isTraceEnabled())
				logger.trace("SELECT o_c_id FROM orders WHERE o_id = " + no_o_id + " AND o_d_id = " + d_id + " AND " +
						"o_w_id = " + w_id);
			try
			{
				ps = statements.createPreparedStatement(con, 27);
				ps.setInt(1, no_o_id);
				ps.setInt(2, d_id);
				ps.setInt(3, w_id);
				rs = ps.executeQuery();
				//rs = proxy.executeQuery(op);

				if(rs!=null && rs.next())
				{
					c_id = rs.getInt(1);
				}

				if(rs!=null)
                                    rs.close();
			} catch(SQLException e)
			{
				lastError = e.getMessage();
				DbUtils.closeQuietly(rs);
				DbUtils.closeQuietly(ps);
				this.rollbackQuietly(con);
				return false;
			}
			op = "UPDATE orders SET o_carrier_id = " + o_carrier_id + " WHERE o_id = " + no_o_id + " AND" +
						" " +
						"o_d_id = " + d_id + " AND o_w_id = " + w_id;
			if(logger.isTraceEnabled())
				logger.trace("UPDATE orders SET o_carrier_id = " + o_carrier_id + " WHERE o_id = " + no_o_id + " AND" +
						" " +
						"o_d_id = " + d_id + " AND o_w_id = " + w_id);
			try
			{
				ps = statements.createPreparedStatement(con, 28);
				ps.setInt(1, o_carrier_id);
				ps.setInt(2, no_o_id);
				ps.setInt(3, d_id);
				ps.setInt(4, w_id);
				ps.executeUpdate();
				//proxy.executeUpdate(op);

			} catch(SQLException e)
			{
				lastError = e.getMessage();
				DbUtils.closeQuietly(rs);
				DbUtils.closeQuietly(ps);
				this.rollbackQuietly(con);
				return false;
			}
			op = "UPDATE order_line SET ol_delivery_d = '" + sdf.format(currentTimeStamp) + "' WHERE " +
						"ol_o_id" +
						" " +
						"=" +
						" " + no_o_id + " AND ol_d_id = " + d_id + " AND ol_w_id = " + w_id;
			if(logger.isTraceEnabled())
				logger.trace("UPDATE order_line SET ol_delivery_d = '" + sdf.format(currentTimeStamp) + "' WHERE " +
						"ol_o_id" +
						" " +
						"=" +
						" " + no_o_id + " AND ol_d_id = " + d_id + " AND ol_w_id = " + w_id);
			try
			{
				ps = statements.createPreparedStatement(con, 29);
				ps.setString(1, currentTimeStamp.toString());
				ps.setInt(2, no_o_id);
				ps.setInt(3, d_id);
				ps.setInt(4, w_id);
				ps.executeUpdate();
				//proxy.executeUpdate(op);

			} catch(SQLException e)
			{
				lastError = e.getMessage();
				DbUtils.closeQuietly(rs);
				DbUtils.closeQuietly(ps);
				this.rollbackQuietly(con);
				return false;
			}
			op = "SELECT SUM(ol_amount) FROM order_line WHERE ol_o_id = " + no_o_id + " AND ol_d_id = " +
						d_id + " AND ol_w_id = " + w_id;
			if(logger.isTraceEnabled())
				logger.trace("SELECT SUM(ol_amount) FROM order_line WHERE ol_o_id = " + no_o_id + " AND ol_d_id = " +
						d_id + " AND ol_w_id = " + w_id);
			try
			{
				ps = statements.createPreparedStatement(con, 30);
				ps.setInt(1, no_o_id);
				ps.setInt(2, d_id);
				ps.setInt(3, w_id);
				rs = ps.executeQuery();
				//rs = proxy.executeQuery(op);
				if(rs!=null && rs.next())
				{
					ol_total = rs.getFloat(1);
				}

				if(rs!=null)
                                    rs.close();
			} catch(SQLException e)
			{
				lastError = e.getMessage();
				DbUtils.closeQuietly(rs);
				DbUtils.closeQuietly(ps);
				this.rollbackQuietly(con);
				return false;
			}
			op = "UPDATE customer SET c_balance = c_balance + " + ol_total + ", c_delivery_cnt = " +
						"c_delivery_cnt + 1 WHERE c_id = " + c_id + " AND c_d_id = " + d_id + " AND " +
						"c_w_id" +
						" = " + w_id;
			if(logger.isTraceEnabled())
				logger.trace("UPDATE customer SET c_balance = c_balance + " + ol_total + ", c_delivery_cnt = " +
						"c_delivery_cnt + 1 WHERE c_id = " + c_id + " AND c_d_id = " + d_id + " AND " +
						"c_w_id" +
						" = " + w_id);
			try
			{
				ps = statements.createPreparedStatement(con, 31);
				ps.setFloat(1, ol_total);
				ps.setInt(2, c_id);
				ps.setInt(3, d_id);
				ps.setInt(4, w_id);
				ps.executeUpdate();
				//proxy.executeUpdate(op);

			} catch(SQLException e)
			{
				lastError = e.getMessage();
				DbUtils.closeQuietly(rs);
				DbUtils.closeQuietly(ps);
				this.rollbackQuietly(con);
				return false;
			}
		}

		return true;
	}

	@Override
	public boolean isReadOnly()
	{
		return false;
	}

	private DeliveryMetadata createDeliveryMetadata()
	{
		int warehouseId = GeneratorUtils.randomNumberIncludeBoundaries(1, TpccConstants.WAREHOUSES_NUMBER);
		int carrierId = GeneratorUtils.randomNumberIncludeBoundaries(1, 10);

		if(warehouseId < 1 || warehouseId > TpccConstants.WAREHOUSES_NUMBER)
		{
			logger.error("invalid warehouse id: {}", warehouseId);
			return null;
		}

		return new DeliveryMetadata(warehouseId, carrierId);
	}

	/**
	 * Created by dnlopes on 15/09/15.
	 */
	private class DeliveryMetadata
	{

		private final int warehouseId;
		private final int carrierId;

		public DeliveryMetadata(int warehouseId, int carriedId)
		{
			this.warehouseId = warehouseId;
			this.carrierId = carriedId;
		}

		public int getWarehouseId()
		{
			return warehouseId;
		}

		public int getCarrierId()
		{
			return carrierId;
		}

	}
}
