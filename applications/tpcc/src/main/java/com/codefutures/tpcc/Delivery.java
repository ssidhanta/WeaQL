package com.codefutures.tpcc;


import java.sql.*;
import java.util.Calendar;
import java.util.Date;

import org.apache.commons.dbutils.DbUtils;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;


public class Delivery implements TpccConstants
{

	private static final Logger logger = LoggerFactory.getLogger(Delivery.class);
	private static final boolean DEBUG = logger.isDebugEnabled();
	private static final boolean TRACE = logger.isTraceEnabled();

	private TpccStatements pStmts;
	private ResultSet rs;
	private PreparedStatement ps;

	public String getLastError()
	{
		return lastError;
	}

	private String lastError;

	public Delivery(TpccStatements pStmts)
	{
		this.pStmts = pStmts;
	}

	public int delivery(int w_id_arg, int o_carrier_id_arg)
	{
		if(DEBUG)
			logger.debug("Transaction:	Delivery");

		int w_id = w_id_arg;
		int o_carrier_id = o_carrier_id_arg;
		int d_id = 0;
		int c_id = 0;
		int no_o_id = 0;
		float ol_total = 0;

		Calendar calendar = Calendar.getInstance();
		Date now = calendar.getTime();
		Timestamp currentTimeStamp = new Timestamp(now.getTime());

		for(d_id = 1; d_id <= DIST_PER_WARE; d_id++)
		{
			if(TRACE)
				logger.trace("SELECT COALESCE(MIN(no_o_id),0) FROM new_orders WHERE no_d_id = " + d_id + " AND " +
						"no_w_id" +
						" " +
						"= " + w_id);
			try
			{
				this.ps = pStmts.createPreparedStatement(25);
				ps.setInt(1, d_id);
				ps.setInt(2, w_id);
				this.rs = ps.executeQuery();

				if(rs.next())
				{
					no_o_id = rs.getInt(1);
				}

				this.rs.close();
			} catch(SQLException e)
			{
				lastError = e.getMessage();
				DbUtils.closeQuietly(this.rs);
				DbUtils.closeQuietly(this.ps);
				pStmts.rollback();

				/*logger.error("SELECT COALESCE(MIN(no_o_id),0) FROM new_orders WHERE no_d_id = " + d_id + " AND " +
						"no_w_id" +
						" " +
						"= " + w_id);*/
				return 0;
			}

			if(no_o_id == 0)
				continue;
			else
			{
				if(DEBUG)
					logger.debug("No_o_id did not equal 0 -> " + no_o_id);
			}

			if(TRACE)
				logger.trace("DELETE FROM new_orders WHERE no_o_id = " + no_o_id + " AND no_d_id = " + d_id + " AND " +
								"no_w_id = " + w_id);
			try
			{
				this.ps = pStmts.createPreparedStatement(26);
				ps.setInt(1, no_o_id);
				ps.setInt(2, d_id);
				ps.setInt(3, w_id);
				ps.executeUpdate();

			} catch(SQLException e)
			{
				lastError = e.getMessage();
				DbUtils.closeQuietly(this.rs);
				DbUtils.closeQuietly(this.ps);
				pStmts.rollback();

/*				logger.error("DELETE FROM new_orders WHERE no_o_id = " + no_o_id + " AND no_d_id = " + d_id + " AND " +
								"no_w_id = " + w_id);*/
				return 0;
			}

			if(TRACE)
				logger.trace("SELECT o_c_id FROM orders WHERE o_id = " + no_o_id + " AND o_d_id = " + d_id + " AND " +
								"o_w_id = " + w_id);
			try
			{
				this.ps = pStmts.createPreparedStatement(27);
				ps.setInt(1, no_o_id);
				ps.setInt(2, d_id);
				ps.setInt(3, w_id);
				this.rs = ps.executeQuery();

				if(this.rs.next())
				{
					c_id = this.rs.getInt(1);
				}

				this.rs.close();
			} catch(SQLException e)
			{
				lastError = e.getMessage();
				DbUtils.closeQuietly(this.rs);
				DbUtils.closeQuietly(this.ps);
				pStmts.rollback();

				/*logger.error("SELECT o_c_id FROM orders WHERE o_id = " + no_o_id + " AND o_d_id = " + d_id + " AND " +
								"o_w_id = " + w_id);*/
				return 0;
			}

			if(TRACE)
				logger.trace("UPDATE orders SET o_carrier_id = " + o_carrier_id + " WHERE o_id = " + no_o_id + " AND" +
						" " +
								"o_d_id = " + d_id + " AND o_w_id = " + w_id);
			try
			{
				this.ps = pStmts.createPreparedStatement(28);
				ps.setInt(1, o_carrier_id);
				ps.setInt(2, no_o_id);
				ps.setInt(3, d_id);
				ps.setInt(4, w_id);
				ps.executeUpdate();

			} catch(SQLException e)
			{
				lastError = e.getMessage();
				DbUtils.closeQuietly(this.rs);
				DbUtils.closeQuietly(this.ps);
				pStmts.rollback();

				/*logger.error("UPDATE orders SET o_carrier_id = " + o_carrier_id + " WHERE o_id = " + no_o_id + "
				AND" +
						" " +
								"o_d_id = " + d_id + " AND o_w_id = " + w_id, e);*/
				return 0;
			}

			if(TRACE)
				logger.trace("UPDATE order_line SET ol_delivery_d = " + currentTimeStamp.toString() + " WHERE " +
						"ol_o_id" +
								" " +
								"=" +
								" " + no_o_id + " AND ol_d_id = " + d_id + " AND ol_w_id = " + w_id);
			try
			{
				this.ps = pStmts.createPreparedStatement(29);
				ps.setString(1, currentTimeStamp.toString());
				ps.setInt(2, no_o_id);
				ps.setInt(3, d_id);
				ps.setInt(4, w_id);
				ps.executeUpdate();

			} catch(SQLException e)
			{
				lastError = e.getMessage();
				DbUtils.closeQuietly(this.rs);
				DbUtils.closeQuietly(this.ps);
				pStmts.rollback();

				/*logger.error("UPDATE order_line SET ol_delivery_d = " + currentTimeStamp.toString() + " WHERE " +
						"ol_o_id" +
								" " +
								"=" +
								" " + no_o_id + " AND ol_d_id = " + d_id + " AND ol_w_id = " + w_id);*/
				return 0;
			}

			if(TRACE)
				logger.trace("SELECT SUM(ol_amount) FROM order_line WHERE ol_o_id = " + no_o_id + " AND ol_d_id = " +
								d_id + " AND ol_w_id = " + w_id);
			try
			{
				this.ps = pStmts.createPreparedStatement(30);
				ps.setInt(1, no_o_id);
				ps.setInt(2, d_id);
				ps.setInt(3, w_id);
				this.rs = ps.executeQuery();
				if(rs.next())
				{
					ol_total = rs.getFloat(1);
				}

				rs.close();
			} catch(SQLException e)
			{
				lastError = e.getMessage();
				DbUtils.closeQuietly(this.rs);
				DbUtils.closeQuietly(this.ps);
				pStmts.rollback();

				/*logger.error("SELECT SUM(ol_amount) FROM order_line WHERE ol_o_id = " + no_o_id + " AND ol_d_id = " +
								d_id + " AND ol_w_id = " + w_id);*/
				return 0;
			}

			if(TRACE)
				logger.trace("UPDATE customer SET c_balance = c_balance + " + ol_total + ", c_delivery_cnt = " +
						"c_delivery_cnt + 1 WHERE c_id = " + c_id + " AND c_d_id = " + d_id + " AND " +
						"c_w_id" +
						" = " + w_id);
			try
			{
				this.ps = pStmts.createPreparedStatement(31);
				ps.setFloat(1, ol_total);
				ps.setInt(2, c_id);
				ps.setInt(3, d_id);
				ps.setInt(4, w_id);
				ps.executeUpdate();

			} catch(SQLException e)
			{
				lastError = e.getMessage();
				DbUtils.closeQuietly(this.rs);
				DbUtils.closeQuietly(this.ps);
				pStmts.rollback();

				/*logger.error("UPDATE customer SET c_balance = c_balance + " + ol_total + ", c_delivery_cnt = " +
						"c_delivery_cnt + 1 WHERE c_id = " + c_id + " AND c_d_id = " + d_id + " AND " +
						"c_w_id" +
						" = " + w_id);  */
				return 0;
			}
		}

		try
		{
			pStmts.commit();
			return 1;
		} catch(SQLException e)
		{
			lastError = e.getMessage();
			DbUtils.closeQuietly(this.rs);
			DbUtils.closeQuietly(this.ps);
			pStmts.rollback();
			return 0;
		}
	}
}
