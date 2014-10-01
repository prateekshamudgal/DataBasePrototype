package edu.buffalo.cse562;

import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.AllTableColumns;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import edu.buffalo.cse562.Datum.Flt;
import edu.buffalo.cse562.Datum.Int;

public class Aggregation implements Operator 
{	
	Operator input;
	TableView table;
	private HashMap<String, TableView> tv;
	List<ColumnDef> schema;
	String tableName;
	PlainSelect pselect ;
	
	//Operator input;
	List<?> selectedItems;
	List<?> groupByColumnReferences;
	List<?> orderByElements;
	HashMap<String, List> groupByMap = new HashMap<String, List>();
	
	public Aggregation(Operator input, TableView table, PlainSelect pselect,
			HashMap<String, TableView> tv, List<ColumnDef> schema, String tableName)
	{
		this.input = input;
		this.table = table;
		this.tv = tv;
		this.pselect = pselect;
		this.selectedItems = pselect.getSelectItems();
		this.groupByColumnReferences = pselect.getGroupByColumnReferences();
		this.orderByElements = pselect.getOrderByElements();
		this.schema = schema;
		this.tableName = tableName.toUpperCase().split("\\ ")[0];
	}
	
	/**
	 * SELECT ID, FIRSTNAME, LASTNAME, (LASTSEASON-FIRSTSEASON) AS EXPERIENCE FROM PLAYERS
	 */
	public TableView solveSelectItemExpression()
	{
		TableView newTable = new TableView();
		List<ColumnDef> newTableColDef = new ArrayList<ColumnDef>();
		boolean isNewColDefSet = false;
		if(selectedItems.size() == 1)
		{
			if(selectedItems.get(0).toString().equals("*"))
				return table;
		}
		Datum[] tuple = null;
		table.reset();
		Datum[] row = table.readOneTuple();
		boolean done = false;
		while(row!=null)
		{			
			Evaluator proj = new Evaluator(row, tv, pselect.getFromItem().toString().toUpperCase().split("\\ ")[0], schema);
			tuple = new Datum[selectedItems.size()];
			
			int idx = 0;
			for(Object sItem: (List) selectedItems)
			{
				String alias = null;
				if(sItem instanceof SelectExpressionItem)
				{							
					((SelectExpressionItem) sItem).accept(proj);
					tuple[idx++] = proj.getValue();
					alias = ((SelectExpressionItem) sItem).getAlias();
				}
				else if(sItem instanceof AllColumns)
				{
					((AllColumns) sItem).accept(proj);
				}							
				else
				{
					((AllColumns) sItem).accept(proj);
				}
				
				if(!done)
				{
					if(!"".equals(alias) && alias!=null)
					{
						newTableColDef.add(new ColumnDef(alias, null, tableName, null));
						isNewColDefSet = true;
					}
					else
					{
						
						for (int j = 0; j < schema.size(); j++)
						{	isNewColDefSet = false;
							ColumnDef c = (ColumnDef)schema.get(j);
							String str = c.getColumnName();
							if (sItem.toString().equalsIgnoreCase(str))
							{
								isNewColDefSet = true;
								newTableColDef.add(c);
								break;
							}
							
						}
						if(!isNewColDefSet)
						newTableColDef.add(new ColumnDef(sItem.toString(), null, tableName, null));
					}
				}
			
			}
			done=true;
			newTable.setColDef(newTableColDef);
			newTable.addTuple(tuple);
			row = table.readOneTuple();
		}
		return newTable;
	}
	
	public void groupBy()
	{
		//If Sub Select is not null then it should be taken as The Table..
		//Specifically for nba04.sql

		TableView table1;
		
		if(tv.get("SUBSELECT") !=null && !"".equals("SUBSELECT") && tv.get("SUBSELECT").length !=0 )
		{
			table = tv.get("SUBSELECT");
			table1 = tv.get("SUBSELECT");
			
		}
		else
		{
			table = tv.get("TEMPSCHEMA");
			table1 = tv.get("TEMPSCHEMA");
		}
		table1.setColDef(table.coldef);	
		
		if(groupByColumnReferences == null)
			return;		

		for(int i=0; i<table1.length; i++)
		{
			String strGroupBy = "";
			for (int j = 0; j < groupByColumnReferences.size(); j++)
				strGroupBy = strGroupBy + table1.getColumnValue(groupByColumnReferences.get(j).toString().toUpperCase(), i).toString();
			if (groupByMap.containsKey(strGroupBy))
			{
				groupByMap.get(strGroupBy).add(i);
			}
			else
			{
				List<Integer> index = new ArrayList<Integer>();
				index.add(i);
				groupByMap.put(strGroupBy, index);
			}
		}
	}

	public TableView getResult()
	{
		groupBy();
		if(groupByColumnReferences == null)
			return null;		
		
		TableView newTable = new TableView();                                                                                                         
		List<ColumnDef> newTableColDef = new ArrayList<ColumnDef>();					
		boolean done = false;
		for(String key: groupByMap.keySet())
		{
			Datum[] tuple = null;
			tuple = new Datum[selectedItems.size()];
			int idx = 0;
			for (int index = 0; index < groupByMap.get(key).size(); index ++)
			{
				Datum[] row = null;
				row = table.list.get((Integer) groupByMap.get(key).get(index));
				Evaluator proj = new Evaluator(row, tv, pselect.getFromItem().toString().toUpperCase().split("\\ ")[0], schema);
				for(Object sItem: (List<?>) selectedItems)
				{
					String alias = null;
					if(sItem instanceof SelectExpressionItem)
					{							
						
						((SelectExpressionItem) sItem).accept(proj);
						String funcName = proj.getFunctionType();
						switch(funcName.toLowerCase())
						{
							case "count": tuple[idx++ % selectedItems.size()] = new Datum.Int(groupByMap.get(key).size());
									      break;
							case "sum": if (tuple[idx % selectedItems.size()] == null)
								            tuple[idx++ % selectedItems.size()] = proj.getValue();
							            else
							            {
							            	tuple[idx % selectedItems.size()] = new Datum.Flt(((Flt) tuple[idx % selectedItems.size()]).add((Flt) proj.getValue()));
							            	idx++;
							            }
							            break;
							case "avg": Datum cnt = new Datum.Flt((float)index);
										if (tuple[idx % selectedItems.size()] == null)
								            tuple[idx++ % selectedItems.size()] = proj.getValue();
							            else
							            {
							            	cnt = new Datum.Flt(index);
							            	tuple[idx % selectedItems.size()] = new Datum.Flt(((Datum.Flt) tuple[idx % selectedItems.size()]).multiply((Flt) cnt));
							            	cnt = new Datum.Flt(index+1);
								            tuple[idx % selectedItems.size()] = new Datum.Flt(((Datum.Flt) tuple[idx % selectedItems.size()]).add((Flt) proj.getValue()));
								            tuple[idx % selectedItems.size()] = new Datum.Flt(((Datum.Flt) tuple[idx % selectedItems.size()]).divide((Flt) cnt));
								            idx++;
							            }
							            break;
							case "min": System.out.println("min");
							            break;
							case "max": System.out.println("max");
							            break;
							default: 
								tuple[idx++ % selectedItems.size()] = proj.getValue();
								break;
						}
						alias = ((SelectExpressionItem) sItem).getAlias();
						funcName = "";
					}
					else if(sItem instanceof AllColumns)
					{
						((AllColumns) sItem).accept(proj);
					}							
					else
					{
						((AllColumns) sItem).accept(proj);
					}
					
					if(!done)
					{
						if(!"".equals(alias) && alias!=null)
						{
							newTableColDef.add(new ColumnDef(alias, null, tableName, null));
						}
						else
						for (int j = 0; j < schema.size(); j++)
						{
							ColumnDef c = (ColumnDef)schema.get(j);
							String str = c.getColumnName();
							if (sItem.toString().equalsIgnoreCase(str))
							{
								newTableColDef.add(c);
								break;
							} 
						}
				     }
				}
				done=true;
			}
			newTable.setColDef(newTableColDef);
			newTable.addTuple(tuple);
		}
		return newTable;
	}
	
	@Override
	public Datum[] readOneTuple() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void reset() {
		// TODO Auto-generated method stub

	}

}
