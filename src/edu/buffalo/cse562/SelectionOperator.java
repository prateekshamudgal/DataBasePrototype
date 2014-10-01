package edu.buffalo.cse562;

import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.FromItem;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class SelectionOperator implements Operator {

	Operator input;
	Expression condition;
	List<ColumnDef> schema;
	List selectedItems;
	HashMap<String, TableView> tv;
	String tableName;
	List joins;

	public SelectionOperator(Operator input, FromItem fromItem,
			List<ColumnDef> schema, Expression condition, List selectedItems,
			HashMap<String, TableView> tv) {
		this.input = input;
		this.condition = condition;
		this.schema = schema;
		this.selectedItems = selectedItems;
		this.tv = tv;
		this.tableName = fromItem.toString();
	}

	public SelectionOperator(Operator input, FromItem fromItem,
			List<ColumnDef> schema, Expression condition, List selectedItems,
			HashMap<String, TableView> tv, List joins) {
		this.input = input;
		this.condition = condition;
		this.schema = schema;
		this.selectedItems = selectedItems;
		this.tv = tv;
		this.tableName = fromItem.toString().toUpperCase();
		this.joins = joins;
	}

	public Datum[] readOneTuple() 
	{
		if (joins != null) 
		{
			List<Expression> exprList = new ArrayList<Expression>();
			String[] str = condition.toString().toLowerCase().split("and");	
			Set<String> tableList = new HashSet<String>();
			
			for (String s : str) // Parse the expression
			{
				CCJSqlParser parser = new CCJSqlParser(new StringReader(s));
				try 
				{
					exprList.add(parser.Expression());
					//System.out.println(parser.Expression().toString());
				} catch (ParseException e) {
					e.printStackTrace();
				}
			}
			//System.out.println(exprList.toString());
			for (Expression e : exprList) // 1 table join
			{
				if (e instanceof BinaryExpression) 
				{
					if (!(((BinaryExpression) e).getLeftExpression() instanceof Column)
							|| !(((BinaryExpression) e).getRightExpression() instanceof Column)) 
					{
						//System.out.println(((BinaryExpression) e).getLeftExpression().toString());
						CreateOneTableJoin(e);
					} 					
				}
			}
			
			for (Expression e : exprList) // 2 table join
			{
				if (e instanceof BinaryExpression) 
				{
					if (((BinaryExpression) e).getLeftExpression() instanceof Column
							&& ((BinaryExpression) e).getRightExpression() instanceof Column) 
					{
						//System.out.println(((BinaryExpression) e).getLeftExpression().toString());
						//System.out.println(((BinaryExpression) e).getRightExpression().toString());
						CreateTwoTableJoin(e, tableList);
					} 					
				}
			}			
		} 
		else // only 1 table
		{
			TableView tView = new TableView();
			tView.setColDef(schema);
			Datum[] tuple = null;
			do 
			{
				tuple = input.readOneTuple();
				if (tuple == null) {
					break;
				}

				if (condition != null) {
					Evaluator eval = new Evaluator(tuple, tv, tableName, schema);
					condition.accept(eval);

					if (eval.getBool()) {
						tView.addTuple(tuple);
					}
				} else
					tView.addTuple(tuple);
			} while (tuple != null);
			tv.put("TEMPSCHEMA", tView);
		}
		return null;
	}

	public void reset() {
		input.reset();
	}
	
	public void CreateOneTableJoin(Expression e)
	{
		Expression leftExpr = ((BinaryExpression) e).getLeftExpression();
		String t1 = ((Column)leftExpr).getTable().getName().toUpperCase();
		
		if (!"".equals(tv.get("TEMP_" + t1)) && tv.get("TEMP_" + t1) != null)
			t1 = "TEMP_" + t1;

		TableView tbl = new TableView();
		//System.out.println(tv.get(t1.toUpperCase()).length);
		tbl.setColDef(tv.get(t1).getColumnDef());
		FromScanner fs = new FromScanner(tv, t1);
		Operator op = fs.source;
		Datum[] d = null;
		int idx=0;
		do 
		{
			d = op.readOneTuple();
			if (d == null) {
				break;
			}

			Evaluator eval = new Evaluator(d, tv, t1, fs.schema);
			e.accept(eval);

			if (eval.getBool()) 
			{
				tbl.addTuple(d);
				//System.out.println("1 table - "+idx);
			}
			idx++;
		} while (d != null);
		
		if(t1.startsWith("TEMP_"))
			tv.put(t1, tbl);
		else
			tv.put("TEMP_" + t1, tbl);
		//System.out.println("----------------------------------------");
	}	

	public void CreateTwoTableJoin(Expression e, Set<String> tableList) 
	{
		//System.out.println(tableList.size());
		Expression leftExpr = ((BinaryExpression) e).getLeftExpression();
		String t1 = ((Column)leftExpr).getTable().getName().toUpperCase();
		Expression rightExpr = ((BinaryExpression) e).getRightExpression();
		String t2 = ((Column)rightExpr).getTable().getName().toUpperCase();
		//System.out.println(t1);	
		if(tableList.contains(t1))
		{
			tableList.add(t2);
			t1 = "TEMPSCHEMA";
			if (!"".equals(tv.get("TEMP_" + t2)) && tv.get("TEMP_" + t2) != null)
				t2 = "TEMP_" + t2;			
		}
		else if(tableList.contains(t2))
		{
			tableList.add(t1);
			t2 = "TEMPSCHEMA";
			if (!"".equals(tv.get("TEMP_" + t1)) && tv.get("TEMP_" + t1) != null)
				t1 = "TEMP_" + t1;			
		}
		else
		{			
			tableList.add(t1);
			tableList.add(t2);
			if (!"".equals(tv.get("TEMP_" + t1)) && tv.get("TEMP_" + t1) != null)
				t1 = "TEMP_" + t1;			
			if (!"".equals(tv.get("TEMP_" + t2)) && tv.get("TEMP_" + t2) != null)
				t2 = "TEMP_" + t2;			
		}
		TableView table1 = tv.get(t1);
		TableView table2 = tv.get(t2);
		Datum dat1, dat2;
		//System.out.println(table1.length);
		String leftCol = ((Column) ((BinaryExpression) e).getLeftExpression()).getColumnName();
		String rightCol = ((Column) ((BinaryExpression) e).getRightExpression()).getColumnName();
		
		TableView tbl = new TableView();
		tbl.setColDef(table1.getColumnDef());
		tbl.appendColDef(table2.getColumnDef());

		FromScanner fs = new FromScanner(tv, t1);
		Operator op = fs.source;						

		Datum[] d = null;
		int idx = 0;
		do 
		{
			d = op.readOneTuple();
			if (d == null) break;
			dat1 = table1.getColumnValue(leftCol, d); // table1 colmn val
			
			FromScanner fs2 = new FromScanner(tv, t2);						
			Operator op2 = fs2.source;
			
			Datum[] d2;
			int idx2 = 0;
			do 
			{
				d2 = op2.readOneTuple();
				if (d2 == null) break;
				dat2 = table2.getColumnValue(rightCol, d2); // table2 colmn val
				
				Datum[] cols = new Datum[tbl.getColumnDef().size()];
				int count = 0;
				for (int i = 0; i < d.length; i++)
					cols[count++] = d[i];
				for (int j = 0; j < d2.length; j++)
					cols[count++ % tbl.getColumnDef().size()] = d2[j];
				if(dat1.compareTo(dat2) == 0)
				{
					tbl.addTuple(cols);
					//System.out.println(dat1.toString() + "!" + dat2.toString() +" - " + idx +" - "+ idx2);
				}
				idx2++;
			} while (d2 != null);
			
			idx++;
		} while (d != null);
		
		tv.put("TEMPSCHEMA", tbl);
		//System.out.println("----------------------------------------");
	} 				
}
