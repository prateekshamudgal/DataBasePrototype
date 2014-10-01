package edu.buffalo.cse562;

import java.util.List;
import java.util.ArrayList;

import edu.buffalo.cse562.Datum.Flt;

import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;

public class ProjectionNode extends PlanNode.Unary implements Operator {
  List<?> selectedItems;
  protected List<ColumnDef> schema_new;
  
  public ProjectionNode(List<?> selectedItems){ 
	  this.selectedItems = selectedItems;
	  
  }
  
  public static ProjectionNode make(
          PlanNode source, 
          List<?> selectedItems)
  {
	ProjectionNode p = new ProjectionNode(selectedItems);
	p.setChild(source);
	p.schema_new = p.getSchemaVars();
	return p;
  }
  
  public List<ColumnDef> getSchemaVars() 
  {
	  List<ColumnDef> child_schema = getChild().getSchemaVars();
	  List<ColumnDef> newSchema = new ArrayList<ColumnDef>();
	  
	  for (int i = 0; i < selectedItems.size(); i++) 
	  {
		  String tName = null;
		  if(((SelectExpressionItem) selectedItems.get(i)).getExpression() instanceof Column)
			  tName = ((Column)((SelectExpressionItem) selectedItems.get(i)).getExpression()).getTable().getName();
		  
		  String alias = ((SelectExpressionItem)  selectedItems.get(i)).getAlias();
		  if(alias!=null && !"".equals(alias))
		  {
			  newSchema.add(new ColumnDef(alias, null, tName, null));
		  }
		  else
		  {
			  ColumnDef c = getColumnDefFromSchema(selectedItems.get(i).toString(), child_schema);
			  if(c!=null)
					newSchema.add(c);
			  else // no columnName found, so add the sItem
			  {
					newSchema.add(new ColumnDef(selectedItems.get(i).toString(), null, tName, null));
			  }
	      }
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
 
    @Override
	public Datum[] readOneTuple() {
		Datum[] tuple = getChild().readOneTuple();
		Datum[] tuple_proj = new Datum[selectedItems.size()];
		List<ColumnDef> child_schema = getChild().getSchemaVars();
		
		if(selectedItems.size() == 1)  // Select * from tableName;
		{
			if(selectedItems.get(0).toString().equals("*"))
				return tuple;
		}
		
		if (tuple != null) 
		{
			Evaluator projection = new Evaluator(tuple, child_schema);
			int idx = 0;
			for (Object sItem: (List<?>) selectedItems) 
			{
				if(sItem instanceof SelectExpressionItem)
				{							
					((SelectExpressionItem) sItem).accept(projection);
				}
				else if(sItem instanceof AllColumns)
				{
					((AllColumns) sItem).accept(projection);
				}							
				else
				{
					((AllColumns) sItem).accept(projection);
				}
				tuple_proj[idx++] = projection.getValue();										 
			}
			return tuple_proj;
		}	
		return null;
	}
	
	@Override
	public void reset() {
		getChild().reset();
	}

}
