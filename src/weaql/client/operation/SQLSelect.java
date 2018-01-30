package weaql.client.operation;


import weaql.common.util.defaults.DatabaseDefaults;
import weaql.common.util.exception.NotCallableException;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.util.deparser.ExpressionDeParser;
import net.sf.jsqlparser.util.deparser.SelectDeParser;

import java.util.List;


/**
 * Created by dnlopes on 06/12/15.
 */
public class SQLSelect extends SQLOperation
{

	private static final String NOT_DELETED_EXPRESSION = DatabaseDefaults.DELETED_COLUMN + "=" + DatabaseDefaults
			.NOT_DELETED_VALUE;
	protected final Select sqlStat;

	public SQLSelect(Select sqlStat) throws JSQLParserException
	{
		super(SQLOperationType.SELECT, (Table) ((PlainSelect) sqlStat.getSelectBody()).getFromItem());

		List joins = ((PlainSelect) sqlStat.getSelectBody()).getJoins();

		if(joins != null)
			throw new JSQLParserException("multi-table select not supported");

		this.sqlStat = sqlStat;
	}

	public Select getSelect()
	{
		return sqlStat;
	}

	@Override
	public void prepareOperation(String tempTableName)
	{
		StringBuilder mainStorageQuery = new StringBuilder("(");
		StringBuilder buffer = new StringBuilder();

		if(dbTable.getTablePolicy().allowDeletes())
		{
			ExpressionDeParser expressionDeParser = new ExpressionDeParser()
			{
				boolean done = false;

				@Override
				public void visit(AndExpression andExpression)
				{
					if(andExpression.isNot())
						getBuffer().append(" NOT ");

					andExpression.getLeftExpression().accept(this);
					getBuffer().append(" AND ");
					andExpression.getRightExpression().accept(this);

					if(!done)
					{
						getBuffer().append(" ").append(NOT_DELETED_EXPRESSION);
						done = true;
					}
				}

				@Override
				public void visit(EqualsTo equalsTo)
				{
					visitOldOracleJoinBinaryExpression(equalsTo, " = ");

					if(!done)
					{
						getBuffer().append(" AND ").append(NOT_DELETED_EXPRESSION);
						done = true;
					}
				}
			};

			SelectDeParser deparser = new SelectDeParser(expressionDeParser, buffer);
			expressionDeParser.setSelectVisitor(deparser);
			expressionDeParser.setBuffer(buffer);
			sqlStat.getSelectBody().accept(deparser);
			mainStorageQuery.append(buffer.toString());
		}
		else
			mainStorageQuery.append(sqlStat.toString());

		mainStorageQuery.append(")");

		if(dbTable.getTablePolicy().allowInserts())
		{
			mainStorageQuery.append(" UNION (");

			PlainSelect plainSelect = ((PlainSelect) sqlStat.getSelectBody());
			((Table) plainSelect.getFromItem()).setName(tempTableName);
			mainStorageQuery.append(sqlStat.toString());
			mainStorageQuery.append(")");
		}

		sqlString = mainStorageQuery.toString();
	}

	public void prepareOperation()
	{
		appendNotDeletedFilter();
	}

	private void appendNotDeletedFilter()
	{
		StringBuilder buffer = new StringBuilder();

		ExpressionDeParser expressionDeParser = new ExpressionDeParser()
		{
			boolean done = false;

			@Override
			public void visit(AndExpression andExpression)
			{
				if(andExpression.isNot())
					getBuffer().append(" NOT ");

				andExpression.getLeftExpression().accept(this);
				getBuffer().append(" AND ");
				andExpression.getRightExpression().accept(this);

				if(!done)
				{
					getBuffer().append(" ").append(NOT_DELETED_EXPRESSION);
					done = true;
				}
			}

			@Override
			public void visit(EqualsTo equalsTo)
			{
				visitOldOracleJoinBinaryExpression(equalsTo, " = ");

				if(!done)
				{
					getBuffer().append(" AND ").append(NOT_DELETED_EXPRESSION);
					done = true;
				}
			}
		};

		SelectDeParser deparser = new SelectDeParser(expressionDeParser, buffer);
		expressionDeParser.setSelectVisitor(deparser);
		expressionDeParser.setBuffer(buffer);
		sqlStat.getSelectBody().accept(deparser);

		sqlString = buffer.toString();
	}

	@Override
	public SQLUpdate duplicate() throws JSQLParserException
	{
		throw new NotCallableException("SQLUpdate.duplicate method missing implementation");
	}

}
