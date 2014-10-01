package edu.buffalo.cse562;

// JoinNode(R, S): Both R and S can be a pipelined relation stored in memory or a relation stored on disk;
// Implement Sort-Merge Join; 
// IF both R and S are on disk, then do two-pass Sort-Merge Join; (Phase 1: Read R, sort, write sorted sublist; Similar to S; Phase 2: Merge sublists from R and S)
// IF R is in memory (small) and S is on disk, 1) sort R in memory, 2) read S, write sorted sublist of S; 3) read sorted sublists of S into memory, merge them with R;
// IF both R and S are in memory, sort R and S, then merge them;

import java.util.List;
import java.util.ArrayList;

import net.sf.jsqlparser.expression.Expression;


public class JoinNode extends PlanNode.Binary implements Operator {
  
  public Expression joinExpression;
  List<ColumnDef> joinSchema;
  TableView joinResult = new TableView();
 
  public List<ColumnDef> getSchemaVars()
  {
	  List<ColumnDef> schema_lhs = this.lhs.getSchemaVars();
	  List<ColumnDef> schema_rhs = this.rhs.getSchemaVars();
	  List<ColumnDef> newSchema = new ArrayList<ColumnDef>();
	  for (int i = 0; i < schema_lhs.size(); i ++)
		  newSchema.add(schema_lhs.get(i));
	  for (int i = 0; i < schema_rhs.size(); i ++)
		  newSchema.add(schema_rhs.get(i));
	  return newSchema;
  }

  public static JoinNode make(PlanNode lhs, PlanNode rhs){
    JoinNode j = new JoinNode();
    j.setLHS(lhs); j.setRHS(rhs);
    j.joinSchema = j.getSchemaVars();
    return j;
  }
  
  public static JoinNode make(PlanNode lhs, PlanNode rhs, Expression joinExpression){
	    JoinNode j = new JoinNode();
	    j.setLHS(lhs); j.setRHS(rhs);
	    j.joinExpression = joinExpression;
	    j.joinSchema = j.getSchemaVars();
	    j.reset();
	    j.getResult();
	    return j;
  }
  
  public void getResult()
  {
	    List<ColumnDef> newSchema = getSchemaVars();
		Datum[] tuple_lhs = this.lhs.readOneTuple();
		while (tuple_lhs != null)
		{
			this.rhs.reset();
			Datum[] tuple_rhs = this.rhs.readOneTuple();
			while (tuple_rhs != null)
			{
				Datum[] tuple_com = new Datum[tuple_lhs.length + tuple_rhs.length];
				for (int i = 0; i < tuple_lhs.length; i ++)
					tuple_com[i] = tuple_lhs[i];
				for (int i = 0; i < tuple_rhs.length; i ++)
					tuple_com[i + tuple_lhs.length] = tuple_rhs[i];
				Evaluator eval = new Evaluator(tuple_com, newSchema);
				joinExpression.accept(eval);
				
				if (eval.getBool()) {
					joinResult.addTuple(tuple_com);
				}
				//else				
				tuple_rhs = this.rhs.readOneTuple();							
			}
			tuple_lhs = this.lhs.readOneTuple();
		}
		//return joinResult;
  }

	@Override
	public Datum[] readOneTuple() {
		return joinResult.readOneTuple();
	}
	
	@Override
	public void reset() {
		this.lhs.reset();
		this.rhs.reset();	
	}
}
