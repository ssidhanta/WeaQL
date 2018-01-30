package com.codefutures.tpcc;


import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.commons.dbutils.DbUtils;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;


public class Slev implements TpccConstants
{

	private static final Logger logger = LoggerFactory.getLogger(Slev.class);
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

	public Slev(TpccStatements pStms)
	{
		this.pStmts = pStms;
	}

	public int slev(int t_num, int w_id_arg,		/* warehouse id */
					int d_id_arg,		/* district id */
					int level_arg		/* stock level */)
	{
			if(DEBUG)
				logger.debug("Transaction: 	SLEV");
			int w_id = w_id_arg;
			int d_id = d_id_arg;
			int level = level_arg;
			int d_next_o_id = 0;
			int i_count = 0;
			int ol_i_id = 0;

			//Get prepared statement
			//"SELECT d_next_o_id FROM district WHERE d_id = ? AND d_w_id = ?"

			try
			{
				this.ps = pStmts.createPreparedStatement(32);
				ps.setInt(1, d_id);
				ps.setInt(2, w_id);
			/*	if(TRACE)
					logger.trace("SELECT d_next_o_id FROM district WHERE d_id = " + d_id + " AND d_w_id = " + w_id);*/
				this.rs = ps.executeQuery();

				if(rs.next())
				{
					d_next_o_id = rs.getInt(1);
				}
				rs.close();
			} catch(SQLException e)
			{
				lastError = e.getMessage();
				DbUtils.closeQuietly(this.rs);
				DbUtils.closeQuietly(this.ps);
				pStmts.rollback();

				//logger.error("SELECT d_next_o_id FROM district WHERE d_id = " + d_id + " AND d_w_id = " + w_id);
				return 0;
			}

			//Get prepared statement
			//"SELECT DISTINCT ol_i_id FROM order_line WHERE ol_w_id = ? AND ol_d_id = ? AND ol_o_id < ? AND ol_o_id
			// >= (? - 20)"
			try
			{
				this.ps = pStmts.createPreparedStatement(33);
				ps.setInt(1, w_id);
				ps.setInt(2, d_id);
				ps.setInt(3, d_next_o_id);
				ps.setInt(4, d_next_o_id);
				/*if(TRACE)
					logger.trace("SELECT DISTINCT ol_i_id FROM order_line WHERE ol_w_id = " + w_id + " AND ol_d_id =" +
							" " +
									d_id + " AND ol_o_id < " + d_next_o_id +
									" AND ol_o_id >= (" + d_next_o_id + " - 20)");       */
				this.rs = ps.executeQuery();

				while(rs.next())
				{
					ol_i_id = rs.getInt(1);
				}

				rs.close();
			} catch(SQLException e)
			{
				lastError = e.getMessage();
				DbUtils.closeQuietly(this.rs);
				DbUtils.closeQuietly(this.ps);
				pStmts.rollback();

				/*logger.error(
						"SELECT DISTINCT ol_i_id FROM order_line WHERE ol_w_id = " + w_id + " AND ol_d_id = " + d_id +
								" AND ol_o_id < " + d_next_o_id +
								" AND ol_o_id >= (" + d_next_o_id + " - 20)");         */
				return 0;
			}

			//Get prepared statement
			//"SELECT count(*) FROM stock WHERE s_w_id = ? AND s_i_id = ? AND s_quantity < ?"

			try
			{
				this.ps = pStmts.createPreparedStatement(34);
				ps.setInt(1, w_id);
				ps.setInt(2, ol_i_id);
				ps.setInt(3, level);
				//if(TRACE)
					/*logger.trace(
							"SELECT count(*) FROM stock WHERE s_w_id = " + w_id + " AND s_i_id = " + ol_i_id + " AND" +
									" " +
									"s_quantity < " + level); */
				this.rs = ps.executeQuery();
				if(rs.next())
				{
					i_count = rs.getInt(1);
				}

				rs.close();
			} catch(SQLException e)
			{
				lastError = e.getMessage();
				DbUtils.closeQuietly(this.rs);
				DbUtils.closeQuietly(this.ps);
				pStmts.rollback();

				/*logger.error(
						"SELECT count(*) FROM stock WHERE s_w_id = " + w_id + " AND s_i_id = " + ol_i_id + " AND " +
								"s_quantity < " + level);*/
				return 0;
			}

			// Commit.
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
