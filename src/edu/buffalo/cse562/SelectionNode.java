package edu.buffalo.cse562;

import java.util.Iterator;
import java.util.List;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.statement.select.FromItem;

public class SelectionNode extends PlanNode.Unary implements Operator{
	
  protected Expression expression;
  Operator input;
  List<ColumnDef> schema;
  List<?> selectedItems;
  String indexName = null;
  List<Datum[]> tuplesList = null;
  Iterator<Datum[]> iterator = null;
  
  public SelectionNode(Expression where) {
	this.expression = where;
  }
  
  public SelectionNode(FromItem fromItem, List<ColumnDef> schema2,
		Expression expression, List<?> selectedItems2) {
	this.expression = expression;
	this.schema = schema2;
	this.selectedItems = selectedItems2;
  }

  public void setCondition(Expression where) 
  { 
	this.expression = where; 
  }
  
  public Expression getWhere() 
  { 
	return expression; 
  }
  
  public List<ColumnDef> getSchemaVars() {
	  return getChild().getSchemaVars();
  }
  

public static PlanNode make(PlanNode source, Expression condition, String idxName, List<Expression> rangeExpression) {

	SelectionNode s = new SelectionNode(condition);
    s.setChild(source);
    s.schema = s.getSchemaVars(); 
    s.indexName = idxName;
    
    // If index present, initialize iterator for matching tuples which is called from readOneTuple()
    if(idxName != null)
    {
    	IndexScanNode indexScanner = new IndexScanNode(idxName, s.schema);
    	if(rangeExpression.size() > 1) // assuming 2 expressions
    		s.tuplesList = indexScanner.getRangeTuples(rangeExpression.get(0), rangeExpression.get(1));
    	else // assuming 1 expression only
    		s.tuplesList = indexScanner.getRangeTuples(rangeExpression.get(0));
    	
    	s.iterator = s.tuplesList.iterator();    	
    }
    
	return s;
}
  
  public static SelectionNode make(PlanNode source, Expression condition)
  {
    SelectionNode s = new SelectionNode(condition);
    s.setChild(source);
    s.schema = s.getSchemaVars();    
    return s;
  }

  public static SelectionNode make(PlanNode source, FromItem fromItem,
			List<ColumnDef> schema, Expression condition, List<?> selectedItems)
  {
    SelectionNode s = new SelectionNode(fromItem, schema, condition, selectedItems);
    s.setChild(source);
    return s;
  }
  
  public void reset()
  {
	  getChild().reset();
  }
  
  public Datum[] readOneTuple()
  {
	 // iterate over the matching tuples returned by index
	 if(indexName != null && iterator != null)
     {
    	 Datum[] row = null;
    	 while(iterator.hasNext())
    	 {
    		row = iterator.next();
    		
			if (expression != null) 
			{
				Evaluator eval = new Evaluator(row, schema);
				expression.accept(eval);
				if (eval.getBool()) // if condition is satisfied
					break;
				else
					row = null;
			} 
			else 
			{
				break;
			}
    	 }
    	 return row;
     }
     
	 // If not index, read directly from DAT file using BlockScanNode
	 Datum[] tuple = null;
     do 
	 {
	    tuple = getChild().readOneTuple();
		if (tuple == null) 
		{
			break;
		}

		if (expression != null) {
			Evaluator eval = new Evaluator(tuple, schema);
			expression.accept(eval);

			if (eval.getBool()) {
				break;
			}
		} 
		else 
		{
			break;
		}				
	 } while (tuple != null);

	  return tuple;					
	}
}

