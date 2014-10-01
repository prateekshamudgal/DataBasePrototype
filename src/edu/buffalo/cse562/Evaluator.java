package edu.buffalo.cse562;

import java.util.HashMap;
import java.util.List;
import java.util.Stack;

import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.arithmetic.*;
import net.sf.jsqlparser.expression.operators.conditional.*;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.AllTableColumns;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItemVisitor;
import net.sf.jsqlparser.statement.select.SubSelect;

public class Evaluator implements ExpressionVisitor, SelectItemVisitor
{	
	HashMap<String, TableView> tv;
	List<ColumnDef> schema;
	private Column columnName;
	Operator oper;
	
	String funcName = "";
	String tableName="";	
	Datum[] tuple;
	Datum value,leftVal,rightVal;
	boolean isTrue = false;  // to check the current relational computation result
	boolean selectAllColumns = false;
	
	String aggrColumnName = "";
	
	//Stack is our best friend
	Stack<Datum> leftValueStack = new Stack<Datum>();
	Stack<Datum> rightValueStack = new Stack<Datum>();
	
	boolean isFromSelectitem = false;
	
	//For Getting the column for Aggregate fnctions
	int columnIndex = 0;
	
	public Evaluator(Datum[] tuple,HashMap<String, TableView> tv, String tableName,List<ColumnDef> schema) 
	{
		this.tableName = tableName;
		this.tv = tv;
		this.tuple =tuple;
		this.schema = schema;
	}
	
	public Evaluator(Datum[] tuple, List<ColumnDef> schema) 
	{
		this.tuple =tuple;
		this.schema = schema;
	}

	// for SelctionNode
	public Evaluator(Expression arg0)
	{
		arg0.accept(this);
	}
	
	private Datum getColumnValue()
	{
		List<ColumnDef> cols =schema;
		Column colName = columnName;
		String tableName = colName.getTable().getName();
		String colmName = colName.getColumnName();
		if(tableName==null || "".equals(tableName))
		{
			for(int i=0; i< cols.size(); i++)
			{
				if(cols.get(i).getColumnName().equalsIgnoreCase(colmName))
				{
					if(isFromSelectitem)
						value = tuple[i];
					return tuple[i];
				}
			}
		}
		else
		{
			for(int i=0; i< cols.size(); i++)
			{
				if(cols.get(i).getColumnName().equalsIgnoreCase(colmName) && cols.get(i).getTableName().equalsIgnoreCase(tableName))
				{
					if(isFromSelectitem)
						value = tuple[i];
					return tuple[i];
				}
			}
		}
		return null;
	}
	
	/**
	 * Whenever an expression needs to be evaluated, we need to evaluate the left and
	 * right side of the expression, which can be a value or an expression.
	 * As it is to be repeated for every Expression.. writing a separate function.
	 * @param arg0
	 */	
	private void setRightAndLeftValue(BinaryExpression arg0) 
	{
		arg0.getLeftExpression().accept(this);
		Datum l = null;
		if((arg0.getLeftExpression() instanceof Column))		
			l = getColumnValue();			
		else		
			l = value;
		if(l instanceof Datum.Str)
			l = new Datum.Str(((Datum.Str)l).value.toLowerCase());
		leftValueStack.push(l);
		
		Datum r = null;
		arg0.getRightExpression().accept(this);
		if((arg0.getRightExpression() instanceof Column))		
			r = getColumnValue();
		else
			r = value;
		if(r instanceof Datum.Str)
			r = new Datum.Str(((Datum.Str)r).value.toLowerCase());
		rightValueStack.push(r);
	}
	
	@Override
	public void visit(NullValue arg0) {
		throw new UnsupportedOperationException("Not supported yet."); 
	}

	@Override
	public void visit(Function arg0) {
		
		funcName = arg0.getName();
		if(arg0.isAllColumns())
		{
			selectAllColumns = true;
			return;
		}
		else if(arg0.getParameters().getExpressions().get(0) instanceof Column)
		{
			Column c = (Column) arg0.getParameters().getExpressions().get(0);
			
			c.accept(this);
		}
		else if(arg0.getParameters().getExpressions().get(0) instanceof Expression)
		{
			Expression exp = (Expression) arg0.getParameters().getExpressions().get(0);
			exp.accept(this);
		}
		selectAllColumns =false;
	}

	@Override
	public void visit(InverseExpression arg0) {
		 throw new UnsupportedOperationException("Not supported yet."); 
	}

	@Override
	public void visit(JdbcParameter arg0) {
		 throw new UnsupportedOperationException("Not supported yet."); 
	}

	@Override
	public void visit(DoubleValue arg0) {
		value = new Datum.Flt((float)arg0.getValue());
	}

	@Override
	public void visit(LongValue arg0) {
		value = new Datum.Int((int)arg0.getValue());
	}

	@Override
	public void visit(DateValue arg0) {
		value = new Datum.Dt(arg0.getValue());
	}

	@Override
	public void visit(TimeValue arg0) {
		 throw new UnsupportedOperationException("Not supported yet."); 
	}

	@Override
	public void visit(TimestampValue arg0) {
		 throw new UnsupportedOperationException("Not supported yet."); 	
	}

	@Override
	public void visit(Parenthesis arg0) {
		arg0.getExpression().accept(this);
	}

	@Override
	public void visit(StringValue arg0) {
		value = new Datum.Str(arg0.getValue());
	}
	
	@Override
	public void visit(Addition arg0) 
	{
		setRightAndLeftValue(arg0);
		if (leftValueStack.peek() instanceof Datum.Int && rightValueStack.peek() instanceof Datum.Int)
		{
			value = new Datum.Int(((Datum.Int) leftValueStack.pop()).add((Datum.Int)rightValueStack.pop()));
		}
		else if (leftValueStack.peek() instanceof Datum.Int && rightValueStack.peek() instanceof Datum.Flt)
		{
			value = new Datum.Flt(((Datum.Int) leftValueStack.pop()).add((Datum.Flt)rightValueStack.pop()));
		}
		else if (leftValueStack.peek() instanceof Datum.Flt && rightValueStack.peek() instanceof Datum.Int)
		{
			value = new Datum.Flt(((Datum.Flt) leftValueStack.pop()).add((Datum.Int)rightValueStack.pop()));
		}
		else 
		{
			value = new Datum.Flt(((Datum.Flt) leftValueStack.pop()).add((Datum.Flt)rightValueStack.pop()));
		}			
	}

	@Override
	public void visit(Division arg0) {
		setRightAndLeftValue(arg0);
		if (leftValueStack.peek() instanceof Datum.Int && rightValueStack.peek() instanceof Datum.Int)
		{
			value = new Datum.Flt(((Datum.Int) leftValueStack.pop()).divide(((Datum.Int)rightValueStack.pop())));
		}
		else if (leftValueStack.peek() instanceof Datum.Int && rightValueStack.peek() instanceof Datum.Flt)
		{
			value = new Datum.Flt(((Datum.Int) leftValueStack.pop()).divide((Datum.Flt)rightValueStack.pop()));
		}
		else if (leftValueStack.peek() instanceof Datum.Flt && rightValueStack.peek() instanceof Datum.Int)
		{
			value = new Datum.Flt(((Datum.Flt) leftValueStack.pop()).divide((Datum.Int)rightValueStack.pop()));
		}
		else 
		{
			value = new Datum.Flt(((Datum.Flt) leftValueStack.pop()).divide((Datum.Flt)rightValueStack.pop()));
		}			
	}

	@Override
	public void visit(Multiplication arg0) 
	{
		setRightAndLeftValue(arg0);
		if (leftValueStack.peek() instanceof Datum.Int && rightValueStack.peek() instanceof Datum.Int)
		{
			value = new Datum.Int(((Datum.Int) leftValueStack.pop()).multiply((Datum.Int)rightValueStack.pop()));
		}
		else if (leftValueStack.peek() instanceof Datum.Int && rightValueStack.peek() instanceof Datum.Flt)
		{
			value = new Datum.Flt(((Datum.Int) leftValueStack.pop()).multiply((Datum.Flt)rightValueStack.pop()));
		}
		else if (leftValueStack.peek() instanceof Datum.Flt && rightValueStack.peek() instanceof Datum.Int)
		{
			value = new Datum.Flt(((Datum.Flt) leftValueStack.pop()).multiply((Datum.Int)rightValueStack.pop()));
		}
		else 
		{
			value = new Datum.Flt(((Datum.Flt) leftValueStack.pop()).multiply((Datum.Flt)rightValueStack.pop()));
		}			
	}

	@Override
	public void visit(Subtraction arg0) 
	{
		setRightAndLeftValue(arg0);
		if (leftValueStack.peek() instanceof Datum.Int && rightValueStack.peek() instanceof Datum.Int)
		{
			value = new Datum.Int(((Datum.Int) leftValueStack.pop()).subtract((Datum.Int)rightValueStack.pop()));
		}
		else if (leftValueStack.peek() instanceof Datum.Int && rightValueStack.peek() instanceof Datum.Flt)
		{
			value = new Datum.Flt(((Datum.Int) leftValueStack.pop()).subtract((Datum.Flt)rightValueStack.pop()));
		}
		else if (leftValueStack.peek() instanceof Datum.Flt && rightValueStack.peek() instanceof Datum.Int)
		{
			value = new Datum.Flt(((Datum.Flt) leftValueStack.pop()).subtract((Datum.Int)rightValueStack.pop()));
		}
		else 
		{
			value = new Datum.Flt(((Datum.Flt) leftValueStack.pop()).subtract((Datum.Flt)rightValueStack.pop()));
		}		
	}

	@Override
	public void visit(AndExpression arg0) 
	{		
		arg0.getLeftExpression().accept(this);
		boolean left = isTrue;
		arg0.getRightExpression().accept(this);
        boolean right = isTrue;
        isTrue = left && right;
    }

	@Override
	public void visit(OrExpression arg0) 
	{
		arg0.getLeftExpression().accept(this);
		boolean left = isTrue;
		arg0.getRightExpression().accept(this);
        boolean right = isTrue;
        isTrue = left || right;
	}

	@Override
	public void visit(Between arg0) 
	{
		arg0.getLeftExpression().accept(this);		
		arg0.getBetweenExpressionStart().accept(this);
		Datum start = value;
		arg0.getBetweenExpressionEnd().accept(this);
		Datum end = value;
		Datum d = getColumnValue();
        if(d.compareTo(start) >= 0 && d.compareTo(end) <= 0)
        	isTrue = true;
        else
        	isTrue = false;
	}

	@Override
	public void visit(EqualsTo arg0) 
	{	
		setRightAndLeftValue(arg0);
		if(leftValueStack.pop().compareTo(rightValueStack.pop()) == 0)
		{
        	isTrue = true;
		}
        else
        	isTrue = false;		
	}

	@Override
	public void visit(GreaterThan arg0) 
	{
		setRightAndLeftValue(arg0);
		if(leftValueStack.pop().compareTo(rightValueStack.pop()) > 0)
			isTrue = true;
        else
        	isTrue = false;
	}

	@Override
	public void visit(GreaterThanEquals arg0) 
	{
		setRightAndLeftValue(arg0);
		if(leftValueStack.pop().compareTo(rightValueStack.pop()) >= 0)
			isTrue = true;
        else
        	isTrue = false;
	}

	@Override
	public void visit(InExpression arg0) {
		throw new UnsupportedOperationException("Not supported yet."); 
	}

	@Override
	public void visit(IsNullExpression arg0) {
		throw new UnsupportedOperationException("Not supported yet."); 
	}

	@Override
	public void visit(LikeExpression arg0) {
		throw new UnsupportedOperationException("Not supported yet."); 
	}

	@Override
	public void visit(MinorThan arg0) 
	{
		setRightAndLeftValue(arg0);
		if(leftValueStack.pop().compareTo(rightValueStack.pop()) < 0)
			isTrue = true;
        else
        	isTrue = false;
	}

	@Override
	public void visit(MinorThanEquals arg0) 
	{
		setRightAndLeftValue(arg0);
		if(leftValueStack.pop().compareTo(rightValueStack.pop()) <= 0)
			isTrue = true;
        else
        	isTrue = false;
	}

	@Override
	public void visit(NotEqualsTo arg0) {
		setRightAndLeftValue(arg0);
		if(leftValueStack.pop().compareTo(rightValueStack.pop()) != 0)
			isTrue = true;
        else
        	isTrue = false;
	}

	@Override
	public void visit(Column arg0) 
	{
		columnName = arg0;		
		if(isFromSelectitem)
		{
			//Get Value of the column
			getColumnValue(); 
		}		
	}

	@Override
	public void visit(SubSelect arg0) {
		throw new UnsupportedOperationException("Not supported yet."); 
	}

	@Override
	public void visit(CaseExpression arg0) {
		throw new UnsupportedOperationException("Not supported yet."); 
	}

	@Override
	public void visit(WhenClause arg0) {
		throw new UnsupportedOperationException("Not supported yet."); 
	}

	@Override
	public void visit(ExistsExpression arg0) {
		throw new UnsupportedOperationException("Not supported yet."); 
	}

	@Override
	public void visit(AllComparisonExpression arg0) {
		throw new UnsupportedOperationException("Not supported yet."); 
	}

	@Override
	public void visit(AnyComparisonExpression arg0) {
		throw new UnsupportedOperationException("Not supported yet."); 
	}

	@Override
	public void visit(Concat arg0) {
		throw new UnsupportedOperationException("Not supported yet."); 
	}

	@Override
	public void visit(Matches arg0) {
		throw new UnsupportedOperationException("Not supported yet."); 
	}

	@Override
	public void visit(BitwiseAnd arg0) 
	{
		arg0.getLeftExpression().accept(this);
		boolean left = isTrue;
        arg0.getRightExpression().accept(this);
        boolean right = isTrue;
        isTrue = left & right;
	}

	@Override
	public void visit(BitwiseOr arg0) 
	{
		arg0.getLeftExpression().accept(this);
		boolean left = isTrue;
        arg0.getRightExpression().accept(this);
        boolean right = isTrue;
        isTrue = left | right;
	}

	@Override
	public void visit(BitwiseXor arg0) 
	{
		arg0.getLeftExpression().accept(this);
		boolean left = isTrue;
        arg0.getRightExpression().accept(this);
        boolean right = isTrue;
        isTrue = left ^ right;
	}
	
	public boolean getBool() 
	{
		return isTrue;
	}
		
	@Override
	public void visit(AllColumns arg0) 
	{
		selectAllColumns = true;
	}

	@Override
	public void visit(AllTableColumns arg0) 
	{
		System.out.println("All Table Coulmns");
	}

	@Override
	public void visit(SelectExpressionItem arg0) 
	{
		isFromSelectitem = true;
		columnIndex++;
		Expression exp = arg0.getExpression();
		if(!(exp instanceof Function))
		{	
			funcName ="";		
		}
        exp.accept(this);
		isFromSelectitem = false;
	}
	
	// return calculated value for a particular column
	public Datum getValue()
	{
		return value;
	}
	
	public String getFunctionType()
	{
		if("count".equalsIgnoreCase(funcName)|| "sum".equalsIgnoreCase(funcName) || "avg".equalsIgnoreCase(funcName))
			return funcName;
		else
			return "";
	}
	public String getColumnNameaggr()
	{
		return aggrColumnName;
	}
}
