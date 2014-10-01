package edu.buffalo.cse562;

import java.util.List;


public abstract class PlanNode implements Operator
{
   
  public static abstract class Unary extends PlanNode 
  {
    protected PlanNode child = null;
    
    public void setChild(PlanNode child) { this.child = child; }
    public PlanNode getChild() { return this.child; }
  }
  
  public static abstract class Binary extends PlanNode 
  {
    protected PlanNode lhs = null, rhs = null;
    
    public void setLHS(PlanNode lhs) { this.lhs = lhs; }
    public PlanNode getLHS() { return this.lhs; }
    
    public void setRHS(PlanNode rhs) { this.rhs = rhs; }
    public PlanNode getRHS() { return this.rhs; }
  }
  
  public static abstract class Leaf extends PlanNode { }
  
  public abstract List<ColumnDef> getSchemaVars();

	public int getTupleSize() {
		return 0;
	}
}
