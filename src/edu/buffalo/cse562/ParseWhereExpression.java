package edu.buffalo.cse562;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.SubSelect;

public class ParseWhereExpression {
	private HashMap<String, List<Expression>> oneTableJoins = new HashMap<String, List<Expression>>();
	private HashMap<String, List<Expression>> twoTableJoins = new HashMap<String, List<Expression>>();
	private List<Expression> parenthesis = new ArrayList<Expression>();
	private List<Expression> subSelectWhere = new ArrayList<Expression>();

	private List<Expression> expressionList = new ArrayList<Expression>();
	boolean reachedTopExpression = false;
	HashMap<String, TableView> tv = new HashMap<String, TableView>();
	String tableName = "";

	/**
	 * Getter Methods
	 * 
	 * @return
	 */
	public HashMap<String, List<Expression>> getOneTableJoins() {
		return oneTableJoins;
	}

	public HashMap<String, List<Expression>> getTwoTableJoins() {
		return twoTableJoins;
	}

	public List<Expression> getParenthesis() {
		return parenthesis;
	}

	public List<Expression> getSubSelectWhere() {
		return subSelectWhere;
	}

	public List<Expression> getExpressionList() {
		return expressionList;
	}

	/**
	 * Constructor for this class.
	 * 
	 * @param whereCondition
	 */
	public ParseWhereExpression(Expression whereCondition) {

		if (whereCondition instanceof BinaryExpression) {
			if (whereCondition instanceof EqualsTo) // just one condition
				// present in where
			{
				// put in One / Two / Subselect
				// System.out.println("just one condition present in where  --  "+whereCondition);
				setExpressionInMaps(whereCondition);
			} else // There has to be AND in the Where Expression
			{
				getleftExpression(whereCondition);
			}
		}
	}

	public ParseWhereExpression(Expression whereCondition, String tableName) {
		this.tableName = tableName;
		if (whereCondition instanceof BinaryExpression) {
			if (whereCondition instanceof EqualsTo) // just one condition
				// present in where
			{
				// put in One / Two / Subselect
				// System.out.println("just one condition present in where  --  "+whereCondition);
				setExpressionInMaps(whereCondition);
			} else // There has to be AND in the Where Expression
			{
				getleftExpression(whereCondition);
			}
		}

		if (((Column) ((BinaryExpression) whereCondition).getLeftExpression())
				.getTable() == null) {
		}
	}

	/**
	 * Recursively calls itself to get the first condition in Where expression.
	 * Then we keep getting the right expressions and keep putting it in
	 * corresponding Hashmaps or lists.
	 * 
	 * @param condition
	 */
	public void getleftExpression(Expression condition) {
		if (condition instanceof AndExpression) {
			if (!reachedTopExpression) {
				getleftExpression(((AndExpression) condition)
						.getLeftExpression());
				if (reachedTopExpression)
					setExpressionInMaps(((AndExpression) condition)
							.getRightExpression());
			}
		} else {
			reachedTopExpression = true; // Once true .. let it be true..dosen't
			// matter
			// System.out.println("Check One or Two table join  -- "+condition);
			setExpressionInMaps(condition);
		}
	}

	/**
	 * If the passed condition is not a subSselect or OR condition [parenthesis]
	 * then check the join type and set corresponding hashmap
	 * 
	 * @param condition
	 */
	public void setExpressionInMaps(Expression condition) {
		if (condition instanceof Parenthesis) {
			// System.out.println("Parenthesis -- "+condition.toString());
			parenthesis.add(condition);
		} else {
			int type = checkJoinType(condition);
			String leftTable = "";
			String rightTable = "";
			if (type == 3) // Check subselect first
			{
				// System.out.println("Sub select in Where -- "+condition.toString());
				subSelectWhere.add(condition);
			} else if (type == 1) {
				// System.out.println(((Column) ((BinaryExpression)
				// condition).getLeftExpression()).getTable().getWholeTableName());

				/*
				 * if(((Column) ((BinaryExpression)
				 * condition).getLeftExpression()).getTable(). == null) {}
				 */

				leftTable = getExpression(((Column) ((BinaryExpression) condition).getLeftExpression()));

				List<Expression> list;

				if (oneTableJoins.containsKey(leftTable))
					list = oneTableJoins.get(leftTable);
				else
					list = new ArrayList<Expression>();

				// System.out.println("One Table Joins --  KEY --  "+leftTable+"  -- Value --  "+condition.toString());
				list.add(condition);
				oneTableJoins.put(leftTable, list);
			} else if (type == 2) {
				leftTable = getExpression(((Column) ((BinaryExpression) condition).getLeftExpression()));

				rightTable = getExpression(((Column) ((BinaryExpression) condition).getRightExpression()));
				/*leftTable = ((Column) ((BinaryExpression) condition)
						.getLeftExpression()).getTable().getName()
						.toUpperCase();
				rightTable = ((Column) ((BinaryExpression) condition)
						.getRightExpression()).getTable().getName()
						.toUpperCase();*/

				List<Expression> list;

				if (twoTableJoins.containsKey(leftTable + "_" + rightTable))
					list = twoTableJoins.get(leftTable + "_" + rightTable);
				else
					list = new ArrayList<Expression>();

				// System.out.println("Two Table Joins --  KEY --  "+leftTable+"_"+rightTable+"  -- Value --  "+condition.toString());
				list.add(condition);
				twoTableJoins.put(leftTable + "_" + rightTable, list);

				// Add at one more place
				expressionList.add(condition);
			} else
				System.out.println("PROBLEMO!!");
		}

	}

	/**
	 * Checks if the Expression from (level 0) is a one or two table join
	 * 
	 * @param e
	 * @return 1 -- Passed expression is a one table Join A.x = 1 // A.x = A.y 2
	 *         -- Passed expression is a two table Join A.x = B.x 3 -- Passed
	 *         expression is a SubSelect in Where condition. 0 -- Not a binary
	 *         Expression.Should not be returned.check!!
	 */
	public int checkJoinType(Expression e) {
		if (e instanceof BinaryExpression) {
			// Either of the one is a 'Value'
			if (!(((BinaryExpression) e).getLeftExpression() instanceof Column)
					|| !(((BinaryExpression) e).getRightExpression() instanceof Column)) {
				// Special case for subselect in where
				if ((((BinaryExpression) e).getRightExpression() instanceof SubSelect))
					return 3;
				else
					return 1;
			}

			// Both are columns
			else if (((BinaryExpression) e).getLeftExpression() instanceof Column
					&& ((BinaryExpression) e).getRightExpression() instanceof Column) {
				
				String leftTable = getExpression(((Column) ((BinaryExpression) e).getLeftExpression()));

				String rightTable = getExpression(((Column) ((BinaryExpression) e).getRightExpression()));
				/*String leftTable = ((Column) ((BinaryExpression) e)
						.getLeftExpression()).getTable().getName()
						.toUpperCase();
				String rightTable = ((Column) ((BinaryExpression) e)
						.getRightExpression()).getTable().getName()
						.toUpperCase();*/

				// Both columns are of same table
				if (leftTable.equals(rightTable))
					return 1;
				else
					return 2;
			}
		}

		return 0;
	}

	public String getExpression(Column col) {

		if(col.toString().contains("."))
			return col.getTable().getName().toUpperCase();
		else
			return tableName.toUpperCase();
	}
}