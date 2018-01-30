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
public class StockLevelTransaction extends AbstractTransaction implements Transaction
{

	private static final Logger logger = LoggerFactory.getLogger(NewOrderTransaction.class);

	private final BaseBenchmarkOptions options;
	private StockLevelMetadata metadata;

	public StockLevelTransaction(BaseBenchmarkOptions options)
	{
		super(TpccConstants.STOCK_LEVEL_TXN_NAME);

		this.options = options;
	}

	@Override
	public boolean executeTransaction(Connection con)//, SandboxExecutionProxy proxy)
	{
		this.metadata = createStockLevelMetadata();
		String op = null;
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
		int level = this.metadata.getLevel();
		int d_next_o_id = 0;
		int i_count = 0;
		int ol_i_id = 0;

		try
		{
			ps = statements.createPreparedStatement(con, 32);
			ps.setInt(1, d_id);
			ps.setInt(2, w_id);

			if(logger.isTraceEnabled())
				logger.trace("SELECT d_next_o_id FROM district WHERE d_id = '" + d_id + "' AND d_w_id = '" + w_id + "'");
			op = "SELECT d_next_o_id FROM district WHERE d_id = " + d_id + " AND d_w_id = " + w_id;
			rs = ps.executeQuery();
                        //System.out.println("*** StockLevelTransactionproxy:="+proxy);
			//rs = proxy.executeQuery(op);
			if(rs.next())
			{
				d_next_o_id = rs.getInt(1);
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
			ps = statements.createPreparedStatement(con, 33);
			ps.setInt(1, w_id);
			ps.setInt(2, d_id);
			ps.setInt(3, d_next_o_id);
			ps.setInt(4, d_next_o_id);

			if(logger.isTraceEnabled())
				logger.trace("SELECT DISTINCT ol_i_id FROM order_line WHERE ol_w_id = " + w_id + " AND ol_d_id =" +
						" " +
						d_id + " AND ol_o_id < " + d_next_o_id +
						" AND ol_o_id >= (" + d_next_o_id + " - 20)");
			op = "SELECT DISTINCT ol_i_id FROM order_line WHERE ol_w_id = " + w_id + " AND ol_d_id =" +
						" " +
						d_id + " AND ol_o_id < " + d_next_o_id +
						" AND ol_o_id >= (" + d_next_o_id + " - 20)";

			rs = ps.executeQuery();
			//rs = proxy.executeQuery(op);
			while(rs.next())
			{
				ol_i_id = rs.getInt(1);
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
			ps = statements.createPreparedStatement(con, 34);
			ps.setInt(1, w_id);
			ps.setInt(2, ol_i_id);
			ps.setInt(3, level);
			op = "SELECT count(*) FROM stock WHERE s_w_id = " + w_id + " AND s_i_id = " + ol_i_id + " " +
						"AND" +
						" " +
						"s_quantity < " + level;
			if(logger.isTraceEnabled())
				logger.trace("SELECT count(*) FROM stock WHERE s_w_id = " + w_id + " AND s_i_id = " + ol_i_id + " " +
						"AND" +
						" " +
						"s_quantity < " + level);

			rs = ps.executeQuery();
			//rs = proxy.executeQuery(op);
			if(rs.next())
			{
				i_count = rs.getInt(1);
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

	private StockLevelMetadata createStockLevelMetadata()
	{

		int warehouseId = GeneratorUtils.randomNumberIncludeBoundaries(1, TpccConstants.WAREHOUSES_NUMBER);
		int districtId = GeneratorUtils.randomNumberIncludeBoundaries(1, TpccConstants.DISTRICTS_PER_WAREHOUSE);
		int level = GeneratorUtils.randomNumberIncludeBoundaries(10, 20);

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

		return new StockLevelTransaction.StockLevelMetadata(warehouseId, districtId, level);
	}

	/**
	 * Created by dnlopes on 15/09/15.
	 */
	private class StockLevelMetadata
	{

		private final int warehouseId;
		private final int districtId;
		private final int level;

		public StockLevelMetadata(int warehouseId, int districtId, int level)
		{
			this.warehouseId = warehouseId;
			this.districtId = districtId;
			this.level = level;
		}

		public int getWarehouseId()
		{
			return warehouseId;
		}

		public int getLevel()
		{
			return level;
		}

		public int getDistrictId()
		{
			return districtId;
		}

	}
}
