package applications.tpcc.txn;


import applications.AbstractTransaction;
import applications.BaseBenchmarkOptions;
import applications.GeneratorUtils;
import applications.Transaction;
import applications.tpcc.TpccAbortedTransactionException;
import applications.tpcc.TpccBenchmarkOptions;
import applications.tpcc.TpccConstants;
import applications.tpcc.TpccStatements;
import org.apache.commons.dbutils.DbUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import applications.util.SymbolsManager;
//import weaql.client.proxy.SandboxExecutionProxy;
import java.util.Calendar;
import java.util.Date;

import java.sql.*;


/**
 * Created by dnlopes on 05/09/15. Modified by Subhajit Sidhanta 30/08/2017
 */
public class NewOrderTransaction extends AbstractTransaction implements Transaction
{

	private static final Logger logger = LoggerFactory.getLogger(NewOrderTransaction.class);

	private static boolean JOINS_ENABLED = false;

	private String s_dist_01 = null;
	private String s_dist_02 = null;
	private String s_dist_03 = null;
	private String s_dist_04 = null;
	private String s_dist_05 = null;
	private String s_dist_06 = null;
	private String s_dist_07 = null;
	private String s_dist_08 = null;
	private String s_dist_09 = null;
	private String s_dist_10 = null;

	private final TpccBenchmarkOptions options;
	private NewOrderMetadata metadata;
	private SymbolsManager symbolsManager;

	public NewOrderTransaction(BaseBenchmarkOptions options)
	{
		super(TpccConstants.NEW_ORDER_TXN_NAME);

		this.options = (TpccBenchmarkOptions) options;
		this.symbolsManager = new SymbolsManager();
	}

	@Override
	public boolean executeTransaction(Connection con)//, SandboxExecutionProxy proxy)
	{
		this.metadata = createNewOrderMetadata();
                
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
		String c_last = null, c_credit = null;
		float c_discount = 0, w_tax = 0, d_tax = 0;
		int d_next_o_id = 0, o_id = 0, tmp = 0, swp = 0, min_num = 0;
		String o_id_string = null;
		String d_next_o_id_string = null;
		int ol_number = 0;
		float i_price = 0, ol_amount = 0;
		String i_name = null;
		int s_quantity = 0;
		String ol_dist_info = null, s_data = null, i_data = null;

		int[] ol_num_seq = new int[TpccConstants.MAX_NUM_ITEMS];
		int[] stock = new int[TpccConstants.MAX_NUM_ITEMS];
		String[] bg = new String[TpccConstants.MAX_NUM_ITEMS];
		float[] amt = new float[TpccConstants.MAX_NUM_ITEMS];

                
		//Timestamp
		//java.sql.Timestamp time = new Timestamp(System.currentTimeMillis());
		//String currentTimeStamp = "'" + time.toString() + "'";
		//String currentTimeStamp = time.toString();
                Calendar calendar = Calendar.getInstance();
                Date now = calendar.getTime();
		Timestamp currentTimeStamp = new Timestamp(now.getTime());
                java.text.SimpleDateFormat sdf = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                String op = null;
		try
		{
			if(JOINS_ENABLED)
			{
				try
				{
					int column = 1;
					ps = statements.createPreparedStatement(con, 0);
					ps.setInt(column++, this.metadata.getWarehouseId());
					ps.setInt(column++, this.metadata.getWarehouseId());
					ps.setInt(column++, this.metadata.getDistrictId());
					ps.setInt(column++, this.metadata.getCustomerId());
					op = "SELECT c_discount, c_last, c_credit, w_tax FROM customer, warehouse WHERE w_id = " +
										this.metadata.getWarehouseId() + " AND c_w_id = " + this.metadata
										.getWarehouseId() + " AND c_d_id = " + this.metadata.getDistrictId() + " AND" +
										" " +
										"c_id = " +
										this.metadata.getCustomerId();
					//System.out.println("*** NewOrderTransactionproxy op:="+op);
                                        if(logger.isTraceEnabled())
						logger.trace(
								"SELECT c_discount, c_last, c_credit, w_tax FROM customer, warehouse WHERE w_id = " +
										this.metadata.getWarehouseId() + " AND c_w_id = " + this.metadata
										.getWarehouseId() + " AND c_d_id = " + this.metadata.getDistrictId() + " AND" +
										" " +
										"c_id = " +
										this.metadata.getCustomerId());
					
					rs = ps.executeQuery();
                                        
					//rs = proxy.executeQuery(op);
					if(rs.next())
					{
						c_discount = rs.getFloat(1);
						c_last = rs.getString(2);
						c_credit = rs.getString(3);
						w_tax = rs.getFloat(4);
					}

					rs.close();

				} catch(SQLException e)
				{
					this.lastError = e.getMessage();
					DbUtils.closeQuietly(rs);
					DbUtils.closeQuietly(ps);
					this.rollbackQuietly(con);
					return false;
				}
			} else
			{
				logger.debug("joins = false");
				// Running 2 seperate queries here
				try
				{
					int column = 1;
					ps = statements.createPreparedStatement(con, 35);
					ps.setInt(column++, this.metadata.getWarehouseId());
					ps.setInt(column++, this.metadata.getDistrictId());
					ps.setInt(column++, this.metadata.getCustomerId());
					op = "SELECT c_discount, c_last, c_credit FROM customer WHERE c_w_id = " + this.metadata
										.getWarehouseId() + " " +
										"AND " +
										"c_d_id = " + this.metadata.getDistrictId() + " AND c_id = " + this.metadata
										.getCustomerId();
					if(logger.isTraceEnabled())
						logger.trace(
								"SELECT c_discount, c_last, c_credit FROM customer WHERE c_w_id = " + this.metadata
										.getWarehouseId() + " " +
										"AND " +
										"c_d_id = " + this.metadata.getDistrictId() + " AND c_id = " + this.metadata
										.getCustomerId());

					rs = ps.executeQuery();
					//rs = proxy.executeQuery(op);
					if(rs!=null && rs.next())
					{
						c_discount = rs.getFloat(1);
						c_last = rs.getString(2);
						c_credit = rs.getString(3);
					}

					//SELECT w_tax FROM warehouse WHERE w_id = ?
					ps = statements.createPreparedStatement(con, 36);
					ps.setInt(1, this.metadata.getWarehouseId());
					op = "SELECT w_tax FROM warehouse WHERE w_id = " + this.metadata.getWarehouseId();
					if(logger.isTraceEnabled())
						logger.trace("SELECT w_tax FROM warehouse WHERE w_id = " + this.metadata.getWarehouseId());

					rs = ps.executeQuery();
					//rs = proxy.executeQuery(op);
					if(rs!=null && rs.next())
					{
						w_tax = rs.getFloat(1);
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
			}

			try
			{
				op = "SELECT d_next_o_id, d_tax FROM district WHERE d_id = " + this.metadata.getDistrictId() +
									"  " +
									"AND d_w_id " +
									"= " + this.metadata.getWarehouseId() + " FOR UPDATE";
				ps = statements.createPreparedStatement(con, 1);
				ps.setInt(1, this.metadata.getDistrictId());
				ps.setInt(2, this.metadata.getWarehouseId());
				if(logger.isTraceEnabled())
					logger.trace(
							"SELECT d_next_o_id, d_tax FROM district WHERE d_id = " + this.metadata.getDistrictId() +
									"  " +
									"AND d_w_id " +
									"= " + this.metadata.getWarehouseId() + " FOR UPDATE");
				//rs = ps.executeQuery();
				//rs = proxy.executeQuery(op);			
				/*if(rs!=null && rs.next())
				{
					d_next_o_id = rs.getInt(1);
					d_tax = rs.getFloat(2);
				} else
				{
					logger.error(
							"Failed to obtain d_next_o_id. No results to query: " + "SELECT d_next_o_id, d_tax FROM" +
									" " +
									"district WHERE d_id = " + this.metadata.getDistrictId() + "  AND d_w_id = " +
									this.metadata.getWarehouseId());
				}*/

				
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

			if(this.options.isCRDTDriver())
				d_next_o_id_string = this.symbolsManager.getNextSymbol();

			//if(this.options.useSequentialOrderIds())
			if(!this.options.isCRDTDriver())
			//if(true)
			{
				try
				{
                                        d_next_o_id = d_next_o_id + 1;
					op = "UPDATE district SET d_next_o_id = " + d_next_o_id + " WHERE d_id = " + this
										.metadata.getDistrictId() + " AND" +

										" " +
										"d_w_id = " + this.metadata.getWarehouseId();
					ps = statements.createPreparedStatement(con, 2);

					if(this.options.isCRDTDriver())
						ps.setString(1, d_next_o_id_string);
					else
						ps.setInt(1, d_next_o_id);

					ps.setInt(2, this.metadata.getDistrictId());
					ps.setInt(3, this.metadata.getWarehouseId());
					d_next_o_id = Integer.valueOf(d_next_o_id + 1);
                                        if(logger.isTraceEnabled())
						logger.trace(
                                                               
								"UPDATE district SET d_next_o_id = " + d_next_o_id + " WHERE d_id = " + this
										.metadata.getDistrictId() + " AND" +

										" " +
										"d_w_id = " + this.metadata.getWarehouseId());
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
			o_id = d_next_o_id;
			o_id_string = d_next_o_id_string;

			try
			{

				ps = statements.createPreparedStatement(con, 3);
				if(this.options.isCRDTDriver())
					ps.setString(1, o_id_string);
				else
					ps.setInt(1, o_id);

				ps.setInt(2, this.metadata.getDistrictId());
				ps.setInt(3, this.metadata.getWarehouseId());
				ps.setInt(4, this.metadata.getCustomerId());
				ps.setString(5, sdf.format(currentTimeStamp));
				ps.setInt(6, this.metadata.getNumberOfItems());
				ps.setInt(7, this.metadata.getAllOrderLinesLocal());
				op = "INSERT INTO orders (o_id, o_d_id, o_w_id, o_c_id, o_entry_d, o_ol_cnt, o_all_local) " +
									"VALUES(" + o_id + "," + this.metadata.getDistrictId() + "," + this.metadata
									.getWarehouseId() +
									"," + this.metadata.getCustomerId() +
									",'" +
									sdf.format(currentTimeStamp) +
									"'," + this.metadata.getNumberOfItems() + "," + this.metadata.getAllOrderLinesLocal
									() +
									")";
				if(logger.isTraceEnabled())
					logger.trace(
							"INSERT INTO orders (o_id, o_d_id, o_w_id, o_c_id, o_entry_d, o_ol_cnt, o_all_local) " +
									"VALUES(" + o_id + "," + this.metadata.getDistrictId() + "," + this.metadata
									.getWarehouseId() +
									"," + this.metadata.getCustomerId() +
									",'" +
									sdf.format(currentTimeStamp) +
									"'," + this.metadata.getNumberOfItems() + "," + this.metadata.getAllOrderLinesLocal
									() +
									")");
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

			try
			{

				ps = statements.createPreparedStatement(con, 4);

				if(this.options.isCRDTDriver())
					ps.setString(1, o_id_string);
				else
					ps.setInt(1, o_id);

				ps.setInt(2, this.metadata.getDistrictId());
				ps.setInt(3, this.metadata.getWarehouseId());
				op = 
							"INSERT INTO new_orders (no_o_id, no_d_id, no_w_id) VALUES (" + o_id + "," + this.metadata
									.getDistrictId() + "," +
									this.metadata.getWarehouseId() + ")";
				if(logger.isTraceEnabled())
					logger.trace(
							"INSERT INTO new_orders (no_o_id, no_d_id, no_w_id) VALUES (" + o_id + "," + this.metadata
									.getDistrictId() + "," +
									this.metadata.getWarehouseId() + ")");
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

		/* sort orders to avoid DeadLock */
			for(int i = 0; i < this.metadata.getNumberOfItems(); i++)
			{
				ol_num_seq[i] = i;
			}

			for(int i = 0; i < (this.metadata.getNumberOfItems() - 1); i++)
			{
				tmp = (TpccConstants.MAX_NUM_ITEMS + 1) * this.metadata.getWarehouseSuplierItems()[ol_num_seq[i]] +
						this.metadata.getItemsIdsArray()[ol_num_seq[i]];
				min_num = i;
				for(int j = i + 1; j < this.metadata.getNumberOfItems(); j++)
				{
					if((TpccConstants.MAX_NUM_ITEMS + 1) * this.metadata.getWarehouseSuplierItems()[ol_num_seq[j]] +
							this.metadata.getItemsIdsArray()[ol_num_seq[j]] < tmp)
					{
						tmp = (TpccConstants.MAX_NUM_ITEMS + 1) * this.metadata.getWarehouseSuplierItems()
								[ol_num_seq[j]] + this.metadata.getItemsIdsArray()[ol_num_seq[j]];
						min_num = j;
					}
				}
				if(min_num != i)
				{
					swp = ol_num_seq[min_num];
					ol_num_seq[min_num] = ol_num_seq[i];
					ol_num_seq[i] = swp;
				}
			}

			for(ol_number = 1; ol_number <= this.metadata.getNumberOfItems(); ol_number++)
			{
				int ol_supply_w_id = this.metadata.getWarehouseSuplierItems()[ol_num_seq[ol_number - 1]];
				int ol_i_id = this.metadata.getItemsIdsArray()[ol_num_seq[ol_number - 1]];
				int ol_quantity = this.metadata.getQuantities()[ol_num_seq[ol_number - 1]];

				try
				{
					ps = statements.createPreparedStatement(con, 5);
					ps.setInt(1, ol_i_id);
					op = "SELECT i_price, i_name, i_data FROM item WHERE i_id =" + ol_i_id;
					if(logger.isTraceEnabled())
						logger.trace("SELECT i_price, i_name, i_data FROM item WHERE i_id =" + ol_i_id);

					rs = ps.executeQuery();
					//rs = proxy.executeQuery(op);
					if(rs!=null && rs.next())
					{
						i_price = rs.getFloat(1);
						i_name = rs.getString(2);
						i_data = rs.getString(3);
					} else
					{
						if(logger.isDebugEnabled())
							logger.debug("No item found for item id " + ol_i_id);

						throw new TpccAbortedTransactionException();
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

				float[] price = new float[TpccConstants.MAX_NUM_ITEMS];
				String[] iname = new String[TpccConstants.MAX_NUM_ITEMS];

				price[ol_num_seq[ol_number - 1]] = i_price;
				iname[ol_num_seq[ol_number - 1]] = i_name;

				try
				{
					ps = statements.createPreparedStatement(con, 6);
					ps.setInt(1, ol_i_id);
					ps.setInt(2, ol_supply_w_id);
					op = "SELECT s_quantity, s_data, s_dist_01, s_dist_02, s_dist_03, s_dist_04, s_dist_05, " +
										"s_dist_06, s_dist_07, s_dist_08, s_dist_09, s_dist_10 FROM " +
										"stock WHERE s_i_id = " + ol_i_id + " AND s_w_id = " + ol_supply_w_id + " " +
										"FOR" +
										" " +
										"UPDATE";
					if(logger.isTraceEnabled())
						logger.trace(
								"SELECT s_quantity, s_data, s_dist_01, s_dist_02, s_dist_03, s_dist_04, s_dist_05, " +
										"s_dist_06, s_dist_07, s_dist_08, s_dist_09, s_dist_10 FROM " +
										"stock WHERE s_i_id = " + ol_i_id + " AND s_w_id = " + ol_supply_w_id + " " +
										"FOR" +
										" " +
										"UPDATE");
					rs = ps.executeQuery();
					//rs = proxy.executeQuery(op);
					if(rs!=null && rs.next())
					{
						s_quantity = rs.getInt(1);
						s_data = rs.getString(2);
						s_dist_01 = rs.getString(3);
						s_dist_02 = rs.getString(4);
						s_dist_03 = rs.getString(5);
						s_dist_04 = rs.getString(6);
						s_dist_05 = rs.getString(7);
						s_dist_06 = rs.getString(8);
						s_dist_07 = rs.getString(9);
						s_dist_08 = rs.getString(10);
						s_dist_09 = rs.getString(11);
						s_dist_10 = rs.getString(12);
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

				ol_dist_info = this.pickDistInfo(ol_dist_info, this.metadata.getDistrictId());

				stock[ol_num_seq[ol_number - 1]] = s_quantity;

				if((i_data.contains("original")) && (s_data.contains("original")))
				{
					bg[ol_num_seq[ol_number - 1]] = "B";

				} else
				{
					bg[ol_num_seq[ol_number - 1]] = "G";

				}

				if(s_quantity > ol_quantity)
				{
					s_quantity = s_quantity - ol_quantity;
				} else
				{
					s_quantity = s_quantity - ol_quantity + 91;
				}

				try
				{
					op = "UPDATE stock SET s_quantity = " + s_quantity + " WHERE s_i_id = " + ol_i_id + " " +
										"AND" +
										" " +
										"s_w_id = " + ol_supply_w_id;
					ps = statements.createPreparedStatement(con, 7);
					ps.setInt(1, s_quantity);
					ps.setInt(2, ol_i_id);
					ps.setInt(3, ol_supply_w_id);
					if(logger.isTraceEnabled())
						logger.trace(
								"UPDATE stock SET s_quantity = " + s_quantity + " WHERE s_i_id = " + ol_i_id + " " +
										"AND" +
										" " +
										"s_w_id = " + ol_supply_w_id);
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

				ol_amount = ol_quantity * i_price * (1 + w_tax + d_tax) * (1 - c_discount);
				amt[ol_num_seq[ol_number - 1]] = ol_amount;

				try
				{
					//final PreparedStatement pstmt8 = pStmts.getStatement(8);
					ps = statements.createPreparedStatement(con, 8);
					if(this.options.isCRDTDriver())
						ps.setString(1, o_id_string);
					else
						ps.setInt(1, o_id);

					ps.setInt(2, this.metadata.getDistrictId());
					ps.setInt(3, this.metadata.getWarehouseId());
					ps.setInt(4, ol_number);
					ps.setInt(5, ol_i_id);
					ps.setInt(6, ol_supply_w_id);
					ps.setInt(7, ol_quantity);
					ps.setFloat(8, ol_amount);
					ps.setString(9, ol_dist_info);
					op = "INSERT INTO order_line (ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id, " +
								"ol_supply_w_id, ol_quantity, ol_amount, ol_dist_info) " +
								"VALUES (" + o_id + "," + this.metadata.getDistrictId() + "," + this.metadata
								.getWarehouseId() + "," +
								ol_number +
								"," +
								ol_i_id +
								"," +
								ol_supply_w_id + "," + ol_quantity + "," + ol_amount + ",'" +
								ol_dist_info + "')";
					if(logger.isTraceEnabled())
						logger.trace("INSERT INTO order_line (ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id, " +
								"ol_supply_w_id, ol_quantity, ol_amount, ol_dist_info) " +
								"VALUES (" + o_id + "," + this.metadata.getDistrictId() + "," + this.metadata
								.getWarehouseId() + "," +
								ol_number +
								"," +
								ol_i_id +
								"," +
								ol_supply_w_id + "," + ol_quantity + "," + ol_amount + ",'" +
								ol_dist_info + "')");
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
		} catch(TpccAbortedTransactionException ate)
		{
			DbUtils.closeQuietly(rs);
			DbUtils.closeQuietly(ps);
			// Rollback if an aborted transaction, they are intentional in some percentage of cases.
			if(logger.isDebugEnabled())
				logger.debug("Caught TpccAbortedTransactionException");

			this.rollbackQuietly(con);
			return true; // this is not an error!
		}
	}

	@Override
	public boolean isReadOnly()
	{
		return false;
	}

	private String pickDistInfo(String ol_dist_info, int ol_supply_w_id)
	{
		switch(ol_supply_w_id)
		{
		case 1:
			ol_dist_info = s_dist_01;
			break;
		case 2:
			ol_dist_info = s_dist_02;
			break;
		case 3:
			ol_dist_info = s_dist_03;
			break;
		case 4:
			ol_dist_info = s_dist_04;
			break;
		case 5:
			ol_dist_info = s_dist_05;
			break;
		case 6:
			ol_dist_info = s_dist_06;
			break;
		case 7:
			ol_dist_info = s_dist_07;
			break;
		case 8:
			ol_dist_info = s_dist_08;
			break;
		case 9:
			ol_dist_info = s_dist_09;
			break;
		case 10:
			ol_dist_info = s_dist_10;
			break;
		}

		return ol_dist_info;
	}

	private NewOrderMetadata createNewOrderMetadata()
	{
		int warehouseId = GeneratorUtils.randomNumberIncludeBoundaries(1, TpccConstants.WAREHOUSES_NUMBER-1);
		int districtId = GeneratorUtils.randomNumberIncludeBoundaries(1, TpccConstants.DISTRICTS_PER_WAREHOUSE-1);
		int customerId = GeneratorUtils.nuRand(1023, 1, TpccConstants.CUSTOMER_PER_DISTRICT);
		//int orderLinesNumber = 1;
		int orderLinesNumber = GeneratorUtils.randomNumberIncludeBoundaries(5, 15);
		int rbk = GeneratorUtils.randomNumberIncludeBoundaries(1, 100);
		int all_local = 1;

		int[] itemsIds = new int[TpccConstants.MAX_NUM_ITEMS];
		int[] supplierWarehouseIds = new int[TpccConstants.MAX_NUM_ITEMS];
		int[] quantities = new int[TpccConstants.MAX_NUM_ITEMS];

		int notfound = TpccConstants.MAXITEMS + 1;

		for(int i = 0; i < orderLinesNumber; i++)
		{
			itemsIds[i] = GeneratorUtils.nuRand(8191, 1, TpccConstants.MAXITEMS);
			if((i == orderLinesNumber - 1) && (rbk == 1))
			{
				itemsIds[i] = notfound;
			}
			if(TpccConstants.ALLOW_MULTI_WAREHOUSE_TX)
			{
				if(GeneratorUtils.randomNumberIncludeBoundaries(1, 100) <= 85)
				{
					supplierWarehouseIds[i] = warehouseId;
				} else
				{
					int remote = selectRemoteWarehouse(warehouseId);
					supplierWarehouseIds[i] = remote;

					if(remote != warehouseId)
						all_local = 0;
				}
			} else
			{
				supplierWarehouseIds[i] = warehouseId;
			}
			quantities[i] = GeneratorUtils.randomNumberIncludeBoundaries(1, 10);
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

		return new NewOrderMetadata(warehouseId, districtId, customerId, orderLinesNumber, all_local, itemsIds,
				supplierWarehouseIds, quantities);
	}

	private int selectRemoteWarehouse(int home_ware)
	{
		int tmp;

		if(TpccConstants.WAREHOUSES_NUMBER == 1)
			return home_ware;
		while((tmp = GeneratorUtils.randomNumberIncludeBoundaries(1, TpccConstants.WAREHOUSES_NUMBER)) == home_ware)
			;
		return tmp;
	}

	/**
	 * Created by dnlopes on 10/09/15.
	 */
	private class NewOrderMetadata
	{

		private final int warehouseId;
		private final int districtId;
		private final int customerId;
		private final int numberOfItems;
		private final int allOrderLinesLocal;
		private final int[] itemsIdsArray;
		private final int[] warehouseSuplierItems;
		private final int[] quantities;

		public NewOrderMetadata(int warehouseId, int districtId, int customerId, int numberOfItems,
								int allOrderLinesLocal, int[] itemsIdsArray, int[] warehouseSuplierItems,
								int[] quantities)
		{
			this.warehouseId = warehouseId;
			this.districtId = districtId;
			this.customerId = customerId;
			this.numberOfItems = numberOfItems;
			this.allOrderLinesLocal = allOrderLinesLocal;
			this.itemsIdsArray = itemsIdsArray;
			this.warehouseSuplierItems = warehouseSuplierItems;
			this.quantities = quantities;

		}

		public int getWarehouseId()
		{
			return warehouseId;
		}

		public int getDistrictId()
		{
			return districtId;
		}

		public int getCustomerId()
		{
			return customerId;
		}

		public int getNumberOfItems()
		{
			return numberOfItems;
		}

		public int getAllOrderLinesLocal()
		{
			return allOrderLinesLocal;
		}

		public int[] getItemsIdsArray()
		{
			return itemsIdsArray;
		}

		public int[] getWarehouseSuplierItems()
		{
			return warehouseSuplierItems;
		}

		public int[] getQuantities()
		{
			return quantities;
		}

	}
}
