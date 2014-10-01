package edu.buffalo.cse562;

import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import edu.buffalo.cse562.Datum.Flt;

public class AggregationNode extends PlanNode.Unary implements Operator  {
	List<?> selectedItems;
	List<?> groupByColumnReferences;
	List<?> orderByElements;
	List<ColumnDef> agg_schema;
	HashMap<String, Datum[]> groupByMap = new HashMap<String, Datum[]>();
	TableView resultTable = new TableView();
	List<HashMap<String, HashMap<String, Integer>>> distinctMapList = new ArrayList<HashMap<String, HashMap<String, Integer>>>();
	HashMap<String, Integer> distinctMap = new HashMap<String, Integer>();
	HashMap<Integer, Integer> distinctIndexMap = new HashMap<Integer, Integer>();
	HashMap<String, HashMap<String, Integer>> countDistinct = new HashMap<String, HashMap<String, Integer>>();
  
  public AggregationNode(List<?> selectedItems, List<?> groupByColumnReferences, List<?> orderByElements)
  { 
    this.selectedItems = selectedItems;
	this.groupByColumnReferences = groupByColumnReferences;
	this.orderByElements = orderByElements;    
  }
  
  public static AggregationNode make(PlanNode source,
		  List<?> selectedItems, List<?> groupByColumnReferences, List<?> orderByElements)
	  {
	    AggregationNode a = new AggregationNode(selectedItems, groupByColumnReferences, orderByElements);
	    a.setChild(source);
	    a.agg_schema = a.getSchemaVars();
	    a.resultTable = a.getResult();
	    return a;
	  }
  
  public List<ColumnDef> getSchemaVars()
  {
	  List<ColumnDef> child_schema = getChild().getSchemaVars();
	  List<ColumnDef> newSchema = new ArrayList<ColumnDef>();
	  
	  for(Object sItem: (List<?>) selectedItems)
	  {		  
		  String tName = null;
		  if(((SelectExpressionItem) sItem).getExpression() instanceof Column)
			  tName = ((Column)((SelectExpressionItem) sItem).getExpression()).getTable().getName();
		
		  String alias = ((SelectExpressionItem) sItem).getAlias();
		  if(alias!=null && !"".equals(alias))
		  {
			  newSchema.add(new ColumnDef(alias, null, tName, null));
		  }
		  else
		  {
				ColumnDef c = getColumnDefFromSchema(sItem.toString(), child_schema);
				if(c!=null)
					newSchema.add(c);
				else // no columnName found, so add the sItem
				{
					newSchema.add(new ColumnDef(sItem.toString(), null, tName, null));
				}
		  }			
		  
		  if(sItem instanceof SelectExpressionItem)
		  { }
	  }	  
	  return newSchema;
  }
  
  private ColumnDef getColumnDefFromSchema(String sItem, List<ColumnDef> colList)
  {
		String tName = "";
		String cName = sItem;
		if(sItem.contains(".")) // tableName.columnName
		{
			tName = sItem.split("\\.")[0];
			cName = sItem.split("\\.")[1];
		}
		
		if(tName==null || "".equals(tName))
		{
			for (int j = 0; j < colList.size(); j++)
			{
				ColumnDef c = (ColumnDef)colList.get(j);								
				if (cName.equalsIgnoreCase(c.getColumnName()))
				return c;				
			}
		}
		else
		{
			for (int j = 0; j < colList.size(); j++)
			{
				ColumnDef c = (ColumnDef)colList.get(j);
				if(c.getColumnName().equalsIgnoreCase(cName) && c.getTableName().equalsIgnoreCase(tName))
					return c;			
			}
		}		
		return null;
	}
  
  public TableView getResult()
	{
	    TableView newTable = new TableView();
		List<ColumnDef> newTableColDef = getSchemaVars();
		newTable.setColDef(newTableColDef);
		List<ColumnDef> schema = getChild().getSchemaVars();
		Datum[] tuple = new Datum[selectedItems.size()];
		Datum[] row = getChild().readOneTuple();
		
		boolean isAggregateFunc = false;			
		int index = 1; // row count
		
		int idx_d = 0;
		for(int i = 0; i < selectedItems.size(); i++)
		{
			if (selectedItems.get(i) instanceof SelectExpressionItem)
			{
				Expression expr1 = ((SelectExpressionItem) selectedItems.get(i)).getExpression();
				if (expr1 instanceof Function)
					if (((Function) expr1).getName().toString().equalsIgnoreCase("count"))
					{
						distinctIndexMap.put(i, idx_d ++);
						distinctMapList.add(null);
					}
			}
		}
		
		if(groupByColumnReferences == null)
		{
			while(row!=null)
			{			
				Evaluator proj = new Evaluator(row, schema);			
				int idx = 0;
				for(Object sItem: (List<?>) selectedItems)
				{
					//String alias = null;
					if(sItem instanceof SelectExpressionItem)
					{							
						((SelectExpressionItem) sItem).accept(proj);
						String funcName = proj.getFunctionType();
						switch(funcName.toLowerCase())
						{
							case "count": int count_local = 0;
								          if (((Function)((SelectExpressionItem) sItem).getExpression()).isDistinct())
							              {
							            	  String distinct_string = proj.getValue().toString();
							            	  if (!distinctMap.containsKey(distinct_string))
							            		  distinctMap.put(distinct_string, 1);
							            	  count_local = distinctMap.size();							            	  
							              }
								          else
								        	  count_local = index;
								          tuple[idx++ % selectedItems.size()] = new Datum.Int(count_local);
										  isAggregateFunc = true;
									      break;
							case "sum": if (tuple[idx % selectedItems.size()] == null)
								            tuple[idx++ % selectedItems.size()] = proj.getValue();
							            else
							            {
							            	tuple[idx % selectedItems.size()] = new Datum.Flt(((Datum.Flt) tuple[idx % selectedItems.size()]).add((Datum.Flt) proj.getValue()));
							            	idx++;
							            }
										isAggregateFunc = true;
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
										isAggregateFunc = true;
							            break;
							case "min": if (tuple[idx % selectedItems.size()] == null)
								            tuple[idx++ % selectedItems.size()] = proj.getValue();
							            else
							            {
							            	float oldVal = ((Flt)tuple[idx % selectedItems.size()]).toFlt(tuple[idx % selectedItems.size()]).getValue();
							            	float newVal = ((Flt) proj.getValue()).getValue();
							            	tuple[idx % selectedItems.size()] = new Datum.Flt(Math.min(oldVal, newVal));
							            	idx++; 
							            }
										isAggregateFunc = true;
							            break;
							case "max": if (tuple[idx % selectedItems.size()] == null)
								            tuple[idx++ % selectedItems.size()] = proj.getValue();
							            else
							            {
							            	float oldVal = ((Flt)tuple[idx % selectedItems.size()]).toFlt(tuple[idx % selectedItems.size()]).getValue();
							            	float newVal = ((Flt) proj.getValue()).getValue();
							            	tuple[idx % selectedItems.size()] = new Datum.Flt(Math.max(oldVal, newVal));
							            	idx++; 
							            }
										isAggregateFunc = true;
							            break;
							default: 
								tuple[idx++ % selectedItems.size()] = proj.getValue();
								break;
						}
						//alias = ((SelectExpressionItem) sItem).getAlias();
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
				}
				if(!isAggregateFunc) // add tuples only when not aggregate function
				{
					newTable.addTuple(tuple); 				
					tuple = new Datum[selectedItems.size()];
				}
				row = getChild().readOneTuple();
				index++;
			}
			if(isAggregateFunc) // add just one row when aggregate functions
			{
				newTable.addTuple(tuple);
			}
			return newTable;
		}
		else {			
			while(row!=null)
			{
				String strGroupBy = "";
				for (int i = 0; i < groupByColumnReferences.size(); i++) {
					  String tName = ((Column)(groupByColumnReferences.get(i))).getTable().getName();
					  String cName = ((Column)(groupByColumnReferences.get(i))).getColumnName();
					  if(tName==null || "".equals(tName))
						{
							for (int j = 0; j < schema.size(); j++)
							{
								ColumnDef c = (ColumnDef)schema.get(j);								
								if (cName.equalsIgnoreCase(c.getColumnName()))
									strGroupBy = strGroupBy + row[j].toString().toUpperCase();				
							}
						}
						else
						{
							for (int j = 0; j < schema.size(); j++)
							{
								ColumnDef c = (ColumnDef)schema.get(j);
								if(c.getColumnName().equalsIgnoreCase(cName) && c.getTableName().equalsIgnoreCase(tName))
									strGroupBy = strGroupBy + row[j].toString().toUpperCase();			
							}
						}
				}

				Evaluator proj = new Evaluator(row, schema);			
				if (groupByMap.containsKey(strGroupBy)){
					groupByMap.get(strGroupBy)[selectedItems.size()] = new Datum.Int(((Datum.Int)groupByMap.get(strGroupBy)[selectedItems.size()]).add(new Datum.Int(1)));
					for(int i = 0; i < selectedItems.size(); i++)
					{
						Object sItem = selectedItems.get(i); 
						if(sItem instanceof SelectExpressionItem)
						{							
							((SelectExpressionItem) sItem).accept(proj);
							String funcName = proj.getFunctionType();
							switch(funcName.toLowerCase())
							{
								case "count": if (((Function)((SelectExpressionItem) sItem).getExpression()).isDistinct())
								              {
									              if (!countDistinct.get(strGroupBy).containsKey(proj.getValue().toString()))
									              {
									            	  groupByMap.get(strGroupBy)[i] = new Datum.Int(((Datum.Int)groupByMap.get(strGroupBy)[i]).add(new Datum.Int(1)));
									            	  countDistinct.get(strGroupBy).put(proj.getValue().toString(), 1);
									              }								            	
								              }
								              else
								            	  groupByMap.get(strGroupBy)[i] = new Datum.Int(((Datum.Int)groupByMap.get(strGroupBy)[i]).add(new Datum.Int(1)));   
										      break;
								case "sum": groupByMap.get(strGroupBy)[i] = new Datum.Flt(((Datum.Flt) groupByMap.get(strGroupBy)[i]).add((Datum.Flt) proj.getValue()));								    
								            break;
								case "avg": groupByMap.get(strGroupBy)[i] = new Datum.Flt(((Datum.Flt) groupByMap.get(strGroupBy)[i]).add((Datum.Flt) proj.getValue()));
								            break;
								case "min": float oldVal = ((Flt)groupByMap.get(strGroupBy)[i]).getValue();
								            float newVal = ((Flt) proj.getValue()).getValue();
								            groupByMap.get(strGroupBy)[i] = new Datum.Flt(Math.min(oldVal, newVal));
								            break;
								case "max": float oldVal2 = ((Flt)groupByMap.get(strGroupBy)[i]).getValue();
					                        float newVal2 = ((Flt) proj.getValue()).getValue();
					                        groupByMap.get(strGroupBy)[i] = new Datum.Flt(Math.max(oldVal2, newVal2));
								            break;
								default: 
									break;
							}
							
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
					}
					row = getChild().readOneTuple();
				}
				else
				{
					Datum[] groupByTuple = new Datum[selectedItems.size() + 1];
					groupByMap.put(strGroupBy, groupByTuple);
					groupByMap.get(strGroupBy)[selectedItems.size()] = new Datum.Int(1);
					for(int i = 0; i < selectedItems.size(); i++)
					{
						Object sItem = selectedItems.get(i); 
						if(sItem instanceof SelectExpressionItem)
						{							
							((SelectExpressionItem) sItem).accept(proj);
							String funcName = proj.getFunctionType();
							switch(funcName.toLowerCase())
							{
								case "count": if (((Function)((SelectExpressionItem) sItem).getExpression()).isDistinct())
								              {
								            	  String distinct_string = proj.getValue().toString();
								            	  HashMap<String, Integer> distinct_item = new HashMap<String, Integer>();
								            	  distinct_item.put(distinct_string, 1);
								            	  countDistinct.put(strGroupBy, distinct_item);
								              }
									          groupByMap.get(strGroupBy)[i] = new Datum.Int(1);
										      break;
								case "sum": groupByMap.get(strGroupBy)[i] = proj.getValue();								    
								            break;
								case "avg": groupByMap.get(strGroupBy)[i] = proj.getValue();
								            break;
								case "min": groupByMap.get(strGroupBy)[i] = proj.getValue();
								            break;
								case "max": groupByMap.get(strGroupBy)[i] = proj.getValue();
								            break;
								default:
									groupByMap.get(strGroupBy)[i] = proj.getValue();
									break;
							}
							
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
					}
					row = getChild().readOneTuple();
				}
			}
			for(String key: groupByMap.keySet())
			{
				tuple = new Datum[selectedItems.size()];
				Datum[] tuple_tmp = groupByMap.get(key);
				for(int i = 0; i < selectedItems.size(); i++)
				{
					Expression expr1 = ((SelectExpressionItem) selectedItems.get(i)).getExpression();
					if (expr1 instanceof Function)
					{
						String func_name = ((Function) expr1).getName();
						if (func_name.toUpperCase().equalsIgnoreCase("avg"))
						{
							if (tuple_tmp[i] instanceof Datum.Int)
								tuple_tmp[i] = new Datum.Flt(((Datum.Int) tuple_tmp[i]).value);
							tuple[i] = new Datum.Flt(((Datum.Flt) tuple_tmp[i]).divide(new Flt(((Datum.Int) tuple_tmp[selectedItems.size()]).value.floatValue())));//new Datum.Flt(((Datum.Flt) tuple[idx % selectedItems.size()]).divide((Flt) cnt));
						}
						else
							tuple[i] = tuple_tmp[i];
					}
					else 
						tuple[i] = tuple_tmp[i];
				}
				newTable.addTuple(tuple);
			}
			return newTable;
		}
	}
  
  public TableView solveSelectItemExpression()
	{
	    TableView newTable = new TableView();
		List<ColumnDef> newTableColDef = getSchemaVars();
		newTable.setColDef(newTableColDef);
		List<ColumnDef> schema = getChild().getSchemaVars();
		Datum[] tuple = new Datum[selectedItems.size()];
		Datum[] row = getChild().readOneTuple();
		
		boolean isAggregateFunc = false;			
		int index = 0; // row count
		while(row!=null)
		{			
			Evaluator proj = new Evaluator(row, schema);			
			int idx = 0;
			for(Object sItem: (List<?>) selectedItems)
			{
				String alias = null;
				if(sItem instanceof SelectExpressionItem)
				{							
					((SelectExpressionItem) sItem).accept(proj);
					String funcName = proj.getFunctionType();
					switch(funcName.toLowerCase())
					{
						case "count": tuple[idx++ % selectedItems.size()] = new Datum.Int(index);
									  isAggregateFunc = true;
								      break;
						case "sum": if (tuple[idx % selectedItems.size()] == null)
							            tuple[idx++ % selectedItems.size()] = proj.getValue();
						            else
						            {
						            	tuple[idx % selectedItems.size()] = new Datum.Flt(((Datum.Flt) tuple[idx % selectedItems.size()]).add((Datum.Flt) proj.getValue()));
						            	idx++;
						            }
									isAggregateFunc = true;
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
									isAggregateFunc = true;
						            break;
						case "min": if (tuple[idx % selectedItems.size()] == null)
							            tuple[idx++ % selectedItems.size()] = proj.getValue();
						            else
						            {
						            	float oldVal = ((Flt)tuple[idx % selectedItems.size()]).toFlt(tuple[idx % selectedItems.size()]).getValue();
						            	float newVal = ((Flt) proj.getValue()).getValue();
						            	tuple[idx % selectedItems.size()] = new Datum.Flt(Math.min(oldVal, newVal));
						            	idx++; 
						            }
									isAggregateFunc = true;
						            break;
						case "max": if (tuple[idx % selectedItems.size()] == null)
							            tuple[idx++ % selectedItems.size()] = proj.getValue();
						            else
						            {
						            	float oldVal = ((Flt)tuple[idx % selectedItems.size()]).toFlt(tuple[idx % selectedItems.size()]).getValue();
						            	float newVal = ((Flt) proj.getValue()).getValue();
						            	tuple[idx % selectedItems.size()] = new Datum.Flt(Math.max(oldVal, newVal));
						            	idx++; 
						            }
									isAggregateFunc = true;
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
			}
			if(!isAggregateFunc) // add tuples only when not aggregate function
			{
				newTable.addTuple(tuple); 				
				tuple = new Datum[selectedItems.size()];
			}
			row = getChild().readOneTuple();
			index++;
		}
		if(isAggregateFunc) // add just one row when aggregate functions
		{
			newTable.addTuple(tuple);
		}
		return newTable;
	}
	    
	@Override
	public Datum[] readOneTuple() {
		if (resultTable != null)
			return resultTable.readOneTuple();
		else return null;
	}
	
	@Override
	public void reset() {
		getChild().reset();
	}
}
