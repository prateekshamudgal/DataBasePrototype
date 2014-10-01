package edu.buffalo.cse562;

import java.util.*;
import java.io.*;

import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.StatementVisitor;
import net.sf.jsqlparser.statement.create.table.*;
import net.sf.jsqlparser.statement.select.*;
import net.sf.jsqlparser.schema.*;

public class FromScanner implements FromItemVisitor 
{
	private File basePath;	
	private String tableName = null;
	private HashMap<String, TableView> tv = null;
	private List joins = null;
	
	public Operator source = null;
	public List<ColumnDef> schema = null;
	
	public FromScanner(File basePath, List<ColumnDefinition> inputDef, String name, String alias) 
	{
		this.basePath = basePath;	
		schema = new ArrayList<ColumnDef>();
		for(int i =0 ;i < inputDef.size() ;i++) // populate schema
		 {	
			ColumnDef colDefObj = new ColumnDef(inputDef.get(i).getColumnName(),inputDef.get(i).getColDataType().getDataType(),name, alias);			
			schema.add(colDefObj);
		}		
		source = new ScanOperator(new File(basePath, name + ".dat"), schema);
		//source = new ScanOperator(new File(basePath, name + ".tbl"), schema);
	}
	
	public FromScanner(HashMap<String, TableView> tv, FromItem fromItem, List joins)
	{
		this.tv = tv;
		this.joins = joins;
		if(fromItem instanceof SubSelect)
		{
			fromItem.accept(this);
		}
		else // check for alias
		{
			String alias = fromItem.getAlias();
			tableName  = fromItem.toString().toUpperCase().split("\\ ")[0];
			if(alias != null && tv.get(tableName) != null)
			{
				tv.put(alias, tv.get(tableName));  // add to main HashMap
			}	
			schema = tv.get(tableName).getColumnDef();
			source = new TupleReadOperator(tv, tableName);
		}
		if(!"".equals(joins) && joins != null)
			CheckJoins();
	}
	
	public FromScanner(HashMap<String, TableView> tv, String tName) // when tableName is known
	{
		this.tv = tv;
		this.tableName = tName;		
		schema = tv.get(tableName).getColumnDef();
		source = new TupleReadOperator(tv, tableName);
	}
	
	public void visit(Table table)
	{
		this.tableName = table.getName();		
		schema = tv.get(tableName).getColumnDef();
		source = new TupleReadOperator(tv, tableName);		
	}	

	public void visit(SubJoin subjoin)
	{
		// TODO
	}
	
	public void visit(SubSelect subselect)
	{		
		if (subselect instanceof SubSelect) 
		{
			SelectBody selectStmt = ((SubSelect) subselect).getSelectBody();			
			if (selectStmt instanceof PlainSelect)
			{
				PlainSelect pselect = (PlainSelect) selectStmt;	
				pselect.getFromItem().accept(this);
				if(tableName != null)
				{
					if(pselect.getWhere() != null)
						source = new SelectionOperator(source, pselect.getFromItem(), schema, 
								pselect.getWhere(), pselect.getJoins(), tv);				
										
					source = new TupleReadOperator(tv, tableName);
					
					CreateNewTable(subselect.getAlias(), pselect.getSelectItems(),pselect); // use Alias as Table name
				}
				else
				{
					System.out.println("Table " + tableName + " not present");
				}
				//if(joins != null) CheckJoins();							
				if(!"".equals(joins) && joins != null) 
					CheckJoins();
			}
			else
				System.out.println("UNIONS NOT HANDLED");						
		}					
	}
	
	public void CheckJoins()
	{
		for(Object obj: joins)
		{
			if(obj instanceof SubSelect)
			{
				((SubSelect) obj).accept(this);
			}
			else // check for alias and add its reference to the TableView
			{
				String alias = null;
				if(((Join) obj).getRightItem() !=null && ((Join) obj).getRightItem().getAlias() != null)
				{
					alias = ((Join) obj).getRightItem().getAlias();
				}
				tableName  = obj.toString().split("\\ ")[0];
				if(alias != null && tv.get(tableName) != null)
				{
					tv.put(alias, tv.get(tableName));  // add to main HashMap
				}	
			}		
		}
	}
	
	public void CreateNewTable(String tName, List selectedItems, PlainSelect pselect)
	{

		TableView newTable = new TableView();	
		List<ColumnDef> colList = new ArrayList<ColumnDef>();
		
		int[] arr = new int[selectedItems.size()];
		int count = 0;
		for (int i = 0; i < selectedItems.size(); i++)
		{
			for (int j = 0; j < schema.size(); j++)
			{
				ColumnDef c = (ColumnDef)schema.get(j);
				String str = c.getColumnName();
				if(selectedItems.get(i).toString().contains("."))
				{
					if(c.getTableName() != null)
						str = c.getTableName().toUpperCase()+"."+str;
				}
										
				if (selectedItems.get(i).toString().equalsIgnoreCase(str))
				{
					ColumnDef col = new ColumnDef(c);
					colList.add(col);
					arr[count++] = j;
					break;
				} 
			}
		}

		Datum[] row = source.readOneTuple();
		//Not sure if we need this yet
		while(row != null)
		{			
			newTable.addTuple(row);
			row = source.readOneTuple();
		}
		
		// To solve Expressions in Select statement	
		Aggregation aggre = new Aggregation(source,newTable, pselect,tv,schema,pselect.getFromItem().toString());
		//Prateeksha:
		newTable = aggre.solveSelectItemExpression();
		
		//newTable.setColDef(colList);
		if("".equals(tName) || tName == null)
			tv.put("SUBSELECT", newTable);
		else
			tv.put(tName, newTable);	
	}

}
