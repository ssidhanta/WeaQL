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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import weaql.client.proxy.SandboxExecutionProxy;

/**
 * Created by dnlopes on 05/09/15. Modified by Subhajit Sidhanta 30/08/2017
 */
public class OrderStatTransaction extends AbstractTransaction implements Transaction
{

	private static final Logger logger = LoggerFactory.getLogger(NewOrderTransaction.class);

	private final BaseBenchmarkOptions options;
	private OrderStatMetadata metadata;

	public OrderStatTransaction(BaseBenchmarkOptions options)
	{
		super(TpccConstants.ORDER_STAT_TXN_NAME);

		this.options = options;
	}

	@Override
	public boolean executeTransaction(Connection con)//, SandboxExecutionProxy proxy)
	{
		this.metadata = createOrderStatMetadata();

		if(this.metadata == null)
		{
			logger.error("failed to generate txn metadata");
			return false;
		}

		try
		{
			con.setReadOnly(true);
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
		int d_id = this.metadata.getDistrictId();
		int c_id = this.metadata.getCustomerId();
		int c_d_id = d_id;
		int c_w_id = w_id;
		String c_first = null;
		String c_middle = null;
		String c_last = null;
		float c_balance = 0;
		int o_id = 0;
		String o_entry_d = null;
		int o_carrier_id = 0;
		int ol_i_id = 0;
		int ol_supply_w_id = 0;
		int ol_quantity = 0;
		float ol_amount = 0;
		String ol_delivery_d = null;
		int namecnt = 0;
		int n = 0;
		String op = null;
		if(this.metadata.getByname() > 0)
		{

			c_last = this.metadata.getLastName();

			try
			{
				ps = statements.createPreparedStatement(con, 20);
				ps.setInt(1, c_w_id);
				ps.setInt(2, c_d_id);
				ps.setString(3, c_last);
				op = "SELECT count(c_id) FROM customer WHERE c_w_id = " + c_w_id + " AND c_d_id = " +
							c_d_id + " AND c_last = " + c_last;
				if(logger.isTraceEnabled())
					logger.trace("SELECT count(c_id) FROM customer WHERE c_w_id = " + c_w_id + " AND c_d_id = " +
							c_d_id + " AND c_last = " + c_last);

				rs = ps.executeQuery();
                                //System.out.println("*** OrderStatTransactionproxy:="+proxy);
				//rs = proxy.executeQuery(op);
				if(rs.next())
				{
					namecnt = rs.getInt(1);
				}

				rs.close();
			} catch(SQLException e)
			{
				lastError = e.getMessage();
				DbUtils.closeQuietly(rs);
				DbUtils.closeQuietly(ps);
				this.rollbackQuietly(con);
				return false;
			}

			try
			{
				ps = statements.createPreparedStatement(con, 21);

				ps.setInt(1, c_w_id);
				ps.setInt(2, c_d_id);
				ps.setString(3, c_last);
				op = "SELECT c_balance, c_first, c_middle, c_last FROM customer WHERE " +
							"c_w_id = " + c_w_id + " AND c_d_id = " + c_d_id + " AND c_last = " + c_last +
							" ORDER" + " BY c_first";
				if(logger.isTraceEnabled())
					logger.trace("SELECT c_balance, c_first, c_middle, c_last FROM customer WHERE " +
							"c_w_id = " + c_w_id + " AND c_d_id = " + c_d_id + " AND c_last = " + c_last +
							" ORDER" + " BY c_first");

				rs = ps.executeQuery();
				//rs = proxy.executeQuery(op);
				if(namecnt % 2 == 1)
				{ //?? Check
					namecnt++;
				} /* Locate midpoint customer; */

				// Use a for loop to find midpoint customer based on namecnt.
				for(n = 0; n < namecnt / 2; n++)
				{
					rs.next();
					c_balance = rs.getFloat(1);
					c_first = rs.getString(2);
					c_middle = rs.getString(3);
					c_last = rs.getString(4);
				}

				rs.close();
			} catch(SQLException e)
			{
				lastError = e.getMessage();
				DbUtils.closeQuietly(rs);
				DbUtils.closeQuietly(ps);
				this.rollbackQuietly(con);
				return false;
			}
		} else
		{		/* by number */

			try
			{
				ps = statements.createPreparedStatement(con, 22);
				ps.setInt(1, c_w_id);
				ps.setInt(2, c_d_id);
				ps.setInt(3, c_id);
				op = "SELECT c_balance, c_first, c_middle, c_last FROM customer WHERE " +
							"c_w_id = " + c_w_id + " AND c_d_id = " + c_d_id + " AND c_id = " + c_id;
				if(logger.isTraceEnabled())
					logger.trace("SELECT c_balance, c_first, c_middle, c_last FROM customer WHERE " +
							"c_w_id = " + c_w_id + " AND c_d_id = " + c_d_id + " AND c_id = " + c_id);

				rs = ps.executeQuery();
				//rs = proxy.executeQuery(op);
				if(rs.next())
				{
					c_balance = rs.getFloat(1);
					c_first = rs.getString(2);
					c_middle = rs.getString(3);
					c_last = rs.getString(4);
				}

				rs.close();
			} catch(SQLException e)
			{
				lastError = e.getMessage();
				DbUtils.closeQuietly(rs);
				DbUtils.closeQuietly(ps);
				this.rollbackQuietly(con);
				return false;
			}
		}

		/* find the most recent order for this customer */

		try
		{
			ps = statements.createPreparedStatement(con, 23);

			ps.setInt(1, c_w_id);
			ps.setInt(2, c_d_id);
			ps.setInt(3, c_id);
			ps.setInt(4, c_w_id);
			ps.setInt(5, c_d_id);
			ps.setInt(6, c_id);
			op = "SELECT o_id, o_entry_d, COALESCE(o_carrier_id,0) FROM orders " +
						"WHERE o_w_id = " + c_w_id + " AND o_d_id = " + c_d_id + " AND o_c_id = " + c_id + " AND " +
						"o_id = " +
						"(SELECT MAX(o_id) FROM orders WHERE o_w_id = " + c_w_id + " AND o_d_id = " + c_d_id + " " +
						"AND o_c_id = " + c_id;
			if(logger.isTraceEnabled())
				logger.trace("SELECT o_id, o_entry_d, COALESCE(o_carrier_id,0) FROM orders " +
						"WHERE o_w_id = " + c_w_id + " AND o_d_id = " + c_d_id + " AND o_c_id = " + c_id + " AND " +
						"o_id = " +
						"(SELECT MAX(o_id) FROM orders WHERE o_w_id = " + c_w_id + " AND o_d_id = " + c_d_id + " " +
						"AND o_c_id = " + c_id);

			rs = ps.executeQuery();
			//rs = proxy.executeQuery(op);
			if(rs.next())
			{
				o_id = rs.getInt(1);
				o_entry_d = rs.getString(2);
				o_carrier_id = rs.getInt(3);
			}

			rs.close();
		} catch(SQLException e)
		{
			lastError = e.getMessage();
			DbUtils.closeQuietly(rs);
			DbUtils.closeQuietly(ps);
			this.rollbackQuietly(con);
			return false;
		}

		//Get prepared statement
		//"SELECT ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_delivery_d FROM order_line WHERE ol_w_id = ?
		// AND ol_d_id = ? AND ol_o_id = ?"
		try
		{
			ps = statements.createPreparedStatement(con, 24);

			ps.setInt(1, c_w_id);
			ps.setInt(2, c_d_id);
			ps.setInt(3, o_id);
			op = "SELECT ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_delivery_d FROM order_line " +
						"WHERE ol_w_id = " + c_w_id + " AND ol_d_id = " + c_d_id + " AND ol_o_id = " +
						o_id;

			if(logger.isTraceEnabled())
				logger.trace("SELECT ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_delivery_d FROM order_line " +
						"WHERE ol_w_id = " + c_w_id + " AND ol_d_id = " + c_d_id + " AND ol_o_id = " +
						o_id);

			rs = ps.executeQuery();
			//rs = proxy.executeQuery(op);
			while(rs.next())
			{
				ol_i_id = rs.getInt(1);
				ol_supply_w_id = rs.getInt(2);
				ol_quantity = rs.getInt(3);
				ol_amount = rs.getFloat(4);
				ol_delivery_d = rs.getString(5);
			}

			rs.close();
		} catch(SQLException e)
		{
			lastError = e.getMessage();
			DbUtils.closeQuietly(rs);
			DbUtils.closeQuietly(ps);
			this.rollbackQuietly(con);
			return false;
		}

		return true;
	}

	@Override
	public boolean isReadOnly()
	{
		return true;
	}

	private OrderStatMetadata createOrderStatMetadata()
	{
		int warehouseId = GeneratorUtils.randomNumberIncludeBoundaries(1, TpccConstants.WAREHOUSES_NUMBER);
		int districtId = GeneratorUtils.randomNumberIncludeBoundaries(1, TpccConstants.DISTRICTS_PER_WAREHOUSE);
		int customerId = GeneratorUtils.nuRand(1023, 1, TpccConstants.CUSTOMER_PER_DISTRICT);
		String c_last = GeneratorUtils.lastName(GeneratorUtils.nuRand(255, 0, 999));
		int byname = 0;

		if(GeneratorUtils.randomNumber(1, 100) <= 60)
		{
			byname = 1; /* select by last name */
		} else
		{
			byname = 0; /* select by customer id */
		}

		if(warehouseId < 1 || warehouseId > TpccConstants.WAREHOUSES_NUMBER)
		{
			logger.error("invalid warehouse id: {}", warehouseId);
			return null;
		}

		if(districtId < 1 || districtId > TpccConstants.DISTRICTS_PER_WAREHOUSE)
		{
			logger.error("invalid district id: {}", districtId);
			return null;
		}

		if(customerId < 1 || customerId > TpccConstants.CUSTOMER_PER_DISTRICT)
		{
			logger.error("invalid customer id: {}", customerId);
			return null;
		}

		return new OrderStatMetadata(warehouseId, districtId, customerId, byname, c_last);
	}

	/**
	 * Created by dnlopes on 15/09/15.
	 */
	private class OrderStatMetadata
	{

		private final int warehouseId;
		private final int districtId;
		private final int customerId;
		private final int byname;

		private final String lastName;

		public OrderStatMetadata(int warehouseId, int districtId, int customerId, int byname, String lastName)
		{
			this.warehouseId = warehouseId;
			this.districtId = districtId;
			this.customerId = customerId;
			this.byname = byname;
			this.lastName = lastName;
		}

		public String getLastName()
		{
			return lastName;
		}

		public int getByname()
		{
			return byname;
		}

		public int getCustomerId()
		{
			return customerId;
		}

		public int getDistrictId()
		{
			return districtId;
		}

		public int getWarehouseId()
		{
			return warehouseId;
		}

	}
}
