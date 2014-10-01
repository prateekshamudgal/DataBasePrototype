package edu.buffalo.cse562;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.create.table.Index;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SubSelect;

//@SuppressWarnings("unchecked")
public class Main {

	static boolean createIndexes = false;
	static File dataDir = null;
	static File indexDir = null;
	static HashMap<String, IndexAttributes> indexStorage = new HashMap<String, IndexAttributes>();
	
	public static void main(String[] args) {
		long time_start = System.currentTimeMillis();

		File dataDir = null, swapDir = null;
		ArrayList<File> sqlFiles = new ArrayList<File>();

		String tableName = null;
		HashMap<String, TableView> tv = new HashMap<String, TableView>();
		TableView tupleList = new TableView();
		
		for (int i = 0; i < args.length; i++)
		{
			if ("--data".equals(args[i])) 
			{
				dataDir = new File(args[i + 1]);
				i++;
			}
			else if ("--index".equals(args[i])) 
			{
				indexDir = new File(args[i + 1]);
				i++;
			}
			else if ("--build".equals(args[i]))
			{
				createIndexes = true;
			}
			else if ("--swap".equals(args[i]))
			{
				swapDir = new File(args[i + 1]);
				i++;
			}
			else
			{
				sqlFiles.add(new File(args[i]));
			}
				
		}
		
		// Create indexes only
		if(createIndexes)
		{
			BuildIndexes.PreCompute(dataDir, indexDir, sqlFiles.get(0));
			return;
		}
		swapDir = null;
		// Iterate over SQLFiles
		for (File sqlF : sqlFiles) 
		{
			try {
				FileReader stream = new FileReader(sqlF);

				CCJSqlParser sqlStatementParser = new CCJSqlParser(stream);
				Statement sqlStatement;

				while ((sqlStatement = sqlStatementParser.Statement()) != null) 
				{
					if (sqlStatement instanceof CreateTable) {
						CreateTable ct = (CreateTable) sqlStatement;

						tableName = ct.getTable().getName().toUpperCase();
						List<ColumnDefinition> colDef = ct.getColumnDefinitions();
						tupleList = new TableView();
						tupleList.setColDef(colDef, tableName, ct.getTable()
								.getAlias());						
						tv.put(tableName, tupleList);
						
						// list of all indexes to be created for a table
						List<Index> indexes = ct.getIndexes();
						if(indexes.size() > 0)
						{
							List<IndexAttributes> indexList = BuildIndexes.BuildIndexList(indexes, 
													tableName, tupleList.getColumnDef());
							for(IndexAttributes attr: indexList)
							{
								indexStorage.put(attr.getIndexName(), attr);
							}
						}
						
					} 
					else if (sqlStatement instanceof Select) 
					{
						SelectBody selectStmt = ((Select) sqlStatement)
								.getSelectBody();

						if (selectStmt instanceof PlainSelect) {
							
							PlainSelect pselect = (PlainSelect) selectStmt;	
						
							PlanNode root = null;
							PlanNode root_child = null;
							ParseWhereExpression exprParser; //
							/*if(tableName.equals(pselect.getFromItem().toString()))
							{
								ParseWhereExpression exprParser2 = new ParseWhereExpression(pselect.getWhere(),tableName);
								exprParser = exprParser2;
							}
							else 
							{
								ParseWhereExpression exprParser2 = new ParseWhereExpression(pselect.getWhere());
								exprParser = exprParser2;
							}*/
							ParseWhereExpression exprParser2 = new ParseWhereExpression(pselect.getWhere());
							exprParser = exprParser2;
							HashMap<String, List<Expression>> oneTblCondition = exprParser.getOneTableJoins();
							List<Expression> parenthesisExpression = exprParser.getParenthesis();
							
							PlainSelect sub_select = null;
							if (pselect.getFromItem() instanceof SubSelect)
							{
								sub_select = (PlainSelect)((SubSelect)pselect.getFromItem()).getSelectBody();
								ParseWhereExpression sub_exprParser = new ParseWhereExpression(sub_select.getWhere());
								HashMap<String, List<Expression>> sub_oneTblCondition = sub_exprParser.getOneTableJoins();

								List<Expression> sub_parenthesisExpression = sub_exprParser.getParenthesis();
								if (sub_select.getJoins() != null)
								{									
									PlanNode joinNode = null;
									HashMap<String, PlanNode> scan_list = new HashMap<String, PlanNode>();
									//For each table, only one IndexScanNode should be created.
									HashMap<String, PlanNode> index_scan_list = new HashMap<String, PlanNode>();
									//For IndexJoin, when using IndexScanNode as rhs, in case of tableName is alias, we have to get the original table name.
									HashMap<String, String> alias_name_map = new HashMap<String, String>();
									//Set alias
									String scan_tblName = ((Table)sub_select.getFromItem()).getName().toUpperCase();
									String alias = scan_tblName;
									List<ColumnDef> scan_schema = tv.get(scan_tblName).coldef;
									
									if (((Table)sub_select.getFromItem()).getAlias() != null)
									{
										alias = ((Table)sub_select.getFromItem()).getAlias().toUpperCase();
										List<ColumnDef> oldCol = tv.get(scan_tblName.toUpperCase()).getColumnDef();
										tv.put(alias, changeTableAlias(alias, oldCol) );
										scan_schema = tv.get(alias).coldef;
										alias_name_map.put(alias, scan_tblName);
									}
									scan_list.put(alias, new BlockScanNode(new File(dataDir, scan_tblName + ".dat"), scan_schema));
									
									if (sub_oneTblCondition.containsKey(scan_tblName.toUpperCase()))
									{
										Expression sel_condition = sub_oneTblCondition.get(scan_tblName.toUpperCase()).get(0);
										String indexName = null;
										List<Expression> rangeExpression = new ArrayList<Expression>();
										indexName = getIndexName(scan_tblName, sub_oneTblCondition.get(scan_tblName.toUpperCase()).get(0));
										if(indexName != null)
											rangeExpression.add(sub_oneTblCondition.get(scan_tblName.toUpperCase()).get(0));										
										for (int i = 1; i < sub_oneTblCondition.get(scan_tblName.toUpperCase()).size(); i ++)
										{	
											String idx = getIndexName(scan_tblName, sub_oneTblCondition.get(scan_tblName.toUpperCase()).get(i));
											if(indexName != null)
											{
												indexName = idx;
												rangeExpression.add(sub_oneTblCondition.get(scan_tblName.toUpperCase()).get(i));
											}
											else
												sel_condition = new AndExpression(sel_condition, sub_oneTblCondition.get(scan_tblName.toUpperCase()).get(i));
										}
										scan_list.put(alias, SelectionNode.make(new BlockScanNode(new File(dataDir, scan_tblName + ".dat"), scan_schema), sel_condition, indexName, rangeExpression));
									}
									else
										scan_list.put(alias, SelectionNode.make(new BlockScanNode(new File(dataDir, scan_tblName + ".dat"), scan_schema), null));								
									for (Object e: sub_select.getJoins())
									{
										if(e instanceof Join)
										{
											//Set alias
											scan_tblName = ((Table)((Join) e).getRightItem()).getName().toUpperCase();
											alias = scan_tblName;
											scan_schema = tv.get(scan_tblName).coldef;
											
											if (((Table)((Join) e).getRightItem()).getAlias() != null)
											{
												alias = ((Table)((Join) e).getRightItem()).getAlias().toUpperCase();
												List<ColumnDef> oldCol = tv.get(scan_tblName.toUpperCase()).getColumnDef();
												tv.put(alias, changeTableAlias(alias, oldCol) );
												scan_schema = tv.get(alias).coldef;
												alias_name_map.put(alias, scan_tblName);
											}
											scan_list.put(alias, new BlockScanNode(new File(dataDir, scan_tblName + ".dat"), scan_schema));
											if (sub_oneTblCondition.containsKey(scan_tblName.toUpperCase()))
											{
												String indexName = null;
												List<Expression> rangeExpression = new ArrayList<Expression>();
												Expression sel_condition = sub_oneTblCondition.get(scan_tblName.toUpperCase()).get(0);
												indexName = getIndexName(scan_tblName, sub_oneTblCondition.get(scan_tblName.toUpperCase()).get(0));
												if(indexName != null)
													rangeExpression.add(sub_oneTblCondition.get(scan_tblName.toUpperCase()).get(0));													
												for (int i = 1; i < sub_oneTblCondition.get(scan_tblName.toUpperCase()).size(); i ++)
												{
													String idx = getIndexName(scan_tblName, sub_oneTblCondition.get(scan_tblName.toUpperCase()).get(i));
													if(idx != null)
													{
														indexName = idx;
														rangeExpression.add(sub_oneTblCondition.get(scan_tblName.toUpperCase()).get(i));													
													}
													else
														sel_condition = new AndExpression(sel_condition, sub_oneTblCondition.get(scan_tblName.toUpperCase()).get(i));													
												}
												scan_list.put(alias, SelectionNode.make(new BlockScanNode(new File(dataDir, scan_tblName + ".dat"), scan_schema), sel_condition, indexName, rangeExpression));
											}
											else
												scan_list.put(alias, SelectionNode.make(new BlockScanNode(new File(dataDir, scan_tblName + ".dat"), scan_schema), null));
											
										}						
									}
									Set<String> tableList = new HashSet<String>();
									List<Expression> expList = sub_exprParser.getExpressionList();
									
									
									for (Expression exp : expList)
								    {
										if (((BinaryExpression) exp).getLeftExpression() instanceof Column
												&& ((BinaryExpression) exp).getRightExpression() instanceof Column) 
										{
									        Expression leftExpr = ((BinaryExpression) exp).getLeftExpression();
											String t1 = ((Column)leftExpr).getTable().getName().toUpperCase();
											Expression rightExpr = ((BinaryExpression) exp).getRightExpression();
											String t2 = ((Column)rightExpr).getTable().getName().toUpperCase();
									        
											if(!tableList.contains(t1) && !tableList.contains(t2))
											{
												//It's the first round of join.
												tableList.add(t1);
												tableList.add(t2);
												// If there is an index present on join condition attribute in either left table or right table, then use IndexLoopJoin
												// Parse join condition
												String leftTable = ((Column) ((BinaryExpression) exp).getLeftExpression()).getTable().getName().toUpperCase();
												String rightTable = ((Column) ((BinaryExpression) exp).getRightExpression()).getTable().getName().toUpperCase();
												String leftCol = ((Column) ((BinaryExpression) exp).getLeftExpression()).getColumnName().toUpperCase();
												String rightCol = ((Column) ((BinaryExpression) exp).getRightExpression()).getColumnName().toUpperCase();
												// check whether left table or right table is an alias, if yes, then change it back to original name;
												if(alias_name_map.containsKey(leftTable))
													leftTable = alias_name_map.get(leftTable);
												if(alias_name_map.containsKey(rightTable))
													rightTable = alias_name_map.get(rightTable);
												if(indexStorage.containsKey(rightTable+"_"+rightCol+"_PK")) {
													//join on Primary key index of rightTable
													PlanNode join_lhs_local = scan_list.get(t1);
													//For first round of join.
													PlanNode join_rhs_local = new IndexScanNode(rightTable+"_"+rightCol+"_PK", tv.get(rightTable).coldef);
													index_scan_list.put(rightTable+"_"+rightCol+"_PK", join_rhs_local);
													joinNode = IndexLoopJoinNode.make(join_lhs_local, join_rhs_local, exp);
													//After index join, we may lose some selection conditions; here pick them up;
													if (oneTblCondition.containsKey(t2.toUpperCase()))
													{
														//Do select again.
														Expression sel_condition = oneTblCondition.get(t2.toUpperCase()).get(0);
														for (int i = 1; i < oneTblCondition.get(t2.toUpperCase()).size(); i++) {
															sel_condition = new AndExpression(sel_condition, oneTblCondition.get(t2.toUpperCase()).get(i));
														}
														joinNode = SelectionNode.make(joinNode, sel_condition, null, null);
													}
													//After IndexLoopJoin, we should modify the last part of its schema
													if (!t2.equalsIgnoreCase(rightTable)){
														//((IndexLoopJoinNode) joinNode).modifyLastPartSchema(tv.get(t2).coldef);
														((IndexLoopJoinNode) joinNode).setSchema(modifyJoinSchema(joinNode.getSchemaVars(), tv.get(t2).coldef));
													}
													
													//System.out.println("join on primary index of right table: " + exp.toString());
												}
												else if (indexStorage.containsKey(leftTable+"_"+leftCol+"_PK")) {
													// join on Primary key index of leftTable
													PlanNode join_lhs_local = scan_list.get(t2);
													PlanNode join_rhs_local = new IndexScanNode(leftTable+"_"+leftCol+"_PK", tv.get(leftTable).coldef);
													index_scan_list.put(leftTable+"_"+leftCol+"_PK", join_rhs_local);
													joinNode = IndexLoopJoinNode.make(join_lhs_local, join_rhs_local, exp);
													if (oneTblCondition.containsKey(t1.toUpperCase()))
													{
														//Do select again.
														Expression sel_condition = oneTblCondition.get(t1.toUpperCase()).get(0);
														for (int i = 1; i < oneTblCondition.get(t1.toUpperCase()).size(); i++) {
															sel_condition = new AndExpression(sel_condition, oneTblCondition.get(t1.toUpperCase()).get(i));
														}
														joinNode = SelectionNode.make(joinNode, sel_condition, null, null);
													}
													//After IndexLoopJoin, we should modify the last part of its schema
													if (!t1.equalsIgnoreCase(leftTable)){
														//((IndexLoopJoinNode) joinNode).modifyLastPartSchema(tv.get(t1).coldef);
														((IndexLoopJoinNode) joinNode).setSchema(modifyJoinSchema(joinNode.getSchemaVars(), tv.get(t1).coldef));
													}
													//System.out.println("join on primary index of left table: " + exp.toString());
												}
												else if (indexStorage.containsKey(rightTable+"_"+rightCol+"_IDX")) {
													// join on secondary index of rightTable
													PlanNode join_lhs_local = scan_list.get(t1);
													PlanNode join_rhs_local = new IndexScanNode(rightTable+"_"+rightCol+"_IDX", tv.get(rightTable).coldef);
													index_scan_list.put(rightTable+"_"+rightCol+"_IDX", join_rhs_local);
													joinNode = IndexLoopJoinNode.make(join_lhs_local, join_rhs_local, exp);
													//After index join, we may lose some selection conditions; here pick them up;
													if (oneTblCondition.containsKey(t2.toUpperCase()))
													{
														//Do select again.
														Expression sel_condition = oneTblCondition.get(t2.toUpperCase()).get(0);
														for (int i = 1; i < oneTblCondition.get(t2.toUpperCase()).size(); i++) {
															sel_condition = new AndExpression(sel_condition, oneTblCondition.get(t2.toUpperCase()).get(i));
														}
														joinNode = SelectionNode.make(joinNode, sel_condition, null, null);
													}
													//After IndexLoopJoin, we should modify the last part of its schema
													if (!t2.equalsIgnoreCase(rightTable)){
														//((IndexLoopJoinNode) joinNode).modifyLastPartSchema(tv.get(t2).coldef);
														((IndexLoopJoinNode) joinNode).setSchema(modifyJoinSchema(joinNode.getSchemaVars(), tv.get(t2).coldef));
													}
													//System.out.println("join on secondary index of right table: " + exp.toString());
												}
												else if (indexStorage.containsKey(leftTable+"_"+leftCol+"_IDX")) {
													// join on secondary index of leftTable
													PlanNode join_lhs_local = scan_list.get(t2);
													PlanNode join_rhs_local = new IndexScanNode(leftTable+"_"+leftCol+"_IDX", tv.get(leftTable).coldef);
													index_scan_list.put(leftTable+"_"+leftCol+"_IDX", join_rhs_local);
													joinNode = IndexLoopJoinNode.make(join_lhs_local, join_rhs_local, exp);
													if (oneTblCondition.containsKey(t1.toUpperCase()))
													{
														//Do select again.
														Expression sel_condition = oneTblCondition.get(t1.toUpperCase()).get(0);
														for (int i = 1; i < oneTblCondition.get(t1.toUpperCase()).size(); i++) {
															sel_condition = new AndExpression(sel_condition, oneTblCondition.get(t1.toUpperCase()).get(i));
														}
														joinNode = SelectionNode.make(joinNode, sel_condition, null, null);
													}
													//After IndexLoopJoin, we should modify the last part of its schema
													if (!t1.equalsIgnoreCase(leftTable)){
														//((IndexLoopJoinNode) joinNode).modifyLastPartSchema(tv.get(t1).coldef);
														((IndexLoopJoinNode) joinNode).setSchema(modifyJoinSchema(joinNode.getSchemaVars(), tv.get(t1).coldef));
													}
													//System.out.println("join on secondary index of left table: " + exp.toString());
												}
												else {
													joinNode = HashJoinNode.make(scan_list.get(t1), scan_list.get(t2), exp, swapDir);
												}
												
												//joinNode = HashJoinNode.make(scan_list.get(t1), scan_list.get(t2), exp, swapDir);
											}
											else if(tableList.contains(t1))
											{
												tableList.add(t2);
												// If rhs of join expression has an index on join attribute, then use indexLoopJoin
												String leftTable = ((Column) ((BinaryExpression) exp).getLeftExpression()).getTable().getName().toUpperCase();
												String rightTable = ((Column) ((BinaryExpression) exp).getRightExpression()).getTable().getName().toUpperCase();
												String leftCol = ((Column) ((BinaryExpression) exp).getLeftExpression()).getColumnName().toUpperCase();
												String rightCol = ((Column) ((BinaryExpression) exp).getRightExpression()).getColumnName().toUpperCase();
												// check whether left table or right table is an alias, if yes, then change it back to original name;
												if(alias_name_map.containsKey(leftTable))
													leftTable = alias_name_map.get(leftTable);
												if(alias_name_map.containsKey(rightTable))
													rightTable = alias_name_map.get(rightTable);
												if(indexStorage.containsKey(rightTable+"_"+rightCol+"_PK")) {
													//join on Primary key index of rightTable
													PlanNode join_lhs_local = joinNode;
													//If IndexScanNode is already there, should new again. But They original one.
													PlanNode join_rhs_local;
													if(index_scan_list.containsKey(rightTable+"_"+rightCol+"_PK"))
													{
														join_rhs_local = index_scan_list.get(rightTable+"_"+rightCol+"_PK");
														// When use IndexScanNode we should reset the schema.
														
														///////////////////////////////////////////////////////////////////////////////////////////////////////////
													}
													else
													{
														join_rhs_local = new IndexScanNode(rightTable+"_"+rightCol+"_PK", tv.get(rightTable).coldef);
														index_scan_list.put(rightTable+"_"+rightCol+"_PK", join_rhs_local);
													}
													///////////////////////////////////////////////////////////////////////////////////////////////////////////////////
											
													joinNode = IndexLoopJoinNode.make(join_lhs_local, join_rhs_local, exp);
													//After index join, we may lose some selection conditions; here pick them up;
													if (oneTblCondition.containsKey(t2.toUpperCase()))
													{
														//Do select again.
														Expression sel_condition = oneTblCondition.get(t2.toUpperCase()).get(0);
														for (int i = 1; i < oneTblCondition.get(t2.toUpperCase()).size(); i++) {
															sel_condition = new AndExpression(sel_condition, oneTblCondition.get(t2.toUpperCase()).get(i));
														}
														joinNode = SelectionNode.make(joinNode, sel_condition, null, null);
													}
													//After IndexLoopJoin, we should modify the last part of its schema
													if (!t2.equalsIgnoreCase(rightTable)){
														//((IndexLoopJoinNode) joinNode).modifyLastPartSchema(tv.get(t2).coldef);
														((IndexLoopJoinNode) joinNode).setSchema(modifyJoinSchema(joinNode.getSchemaVars(), tv.get(t2).coldef));
													}
													//System.out.println("join on primary index of right table: " + exp.toString());
												}
												else if (indexStorage.containsKey(rightTable+"_"+rightCol+"_IDX")) {
													// join on secondary index of rightTable
													PlanNode join_lhs_local = joinNode;
													PlanNode join_rhs_local;
													if(index_scan_list.containsKey(rightTable+"_"+rightCol+"_IDX"))
													{
														join_rhs_local = index_scan_list.get(rightTable+"_"+rightCol+"_IDX");
													}
													else
													{
														join_rhs_local = new IndexScanNode(rightTable+"_"+rightCol+"_IDX", tv.get(rightTable).coldef);
														index_scan_list.put(rightTable+"_"+rightCol+"_IDX", join_rhs_local);
													} 

													joinNode = IndexLoopJoinNode.make(join_lhs_local, join_rhs_local, exp);
													//After index join, we may lose some selection conditions; here pick them up;
													if (oneTblCondition.containsKey(t2.toUpperCase()))
													{
														//Do select again.
														Expression sel_condition = oneTblCondition.get(t2.toUpperCase()).get(0);
														for (int i = 1; i < oneTblCondition.get(t2.toUpperCase()).size(); i++) {
															sel_condition = new AndExpression(sel_condition, oneTblCondition.get(t2.toUpperCase()).get(i));
														}
														joinNode = SelectionNode.make(joinNode, sel_condition, null, null);
													}
													//After IndexLoopJoin, we should modify the last part of its schema
													if (!t2.equalsIgnoreCase(rightTable)){
														//((IndexLoopJoinNode) joinNode).modifyLastPartSchema(tv.get(t2).coldef);
														((IndexLoopJoinNode) joinNode).setSchema(modifyJoinSchema(joinNode.getSchemaVars(), tv.get(t2).coldef));
													}
													//System.out.println("join on secondary index of right table: " + exp.toString());
												}
												else {
													joinNode = HashJoinNode.make(joinNode, scan_list.get(t2), exp, swapDir);
												}
												//joinNode = HashJoinNode.make(joinNode, scan_list.get(t2), exp, swapDir);
												
											}
											else if(tableList.contains(t2)) // swap
											{
												tableList.add(t1);
												// If lhs of join expression has an index on join attribute, then use indexLoopJoin
												String leftTable = ((Column) ((BinaryExpression) exp).getLeftExpression()).getTable().getName().toUpperCase();
												String rightTable = ((Column) ((BinaryExpression) exp).getRightExpression()).getTable().getName().toUpperCase();
												String leftCol = ((Column) ((BinaryExpression) exp).getLeftExpression()).getColumnName().toUpperCase();
												String rightCol = ((Column) ((BinaryExpression) exp).getRightExpression()).getColumnName().toUpperCase();
												// check whether left table or right table is an alias, if yes, then change it back to original name;
												if(alias_name_map.containsKey(leftTable))
													leftTable = alias_name_map.get(leftTable);
												if(alias_name_map.containsKey(rightTable))
													rightTable = alias_name_map.get(rightTable);
												if(indexStorage.containsKey(leftTable+"_"+leftCol+"_PK")) {
													//join on Primary key index of leftTable
													PlanNode join_lhs_local = joinNode;
													//If IndexScanNode is already there, should new again. But They original one.
													PlanNode join_rhs_local;
													if(index_scan_list.containsKey(leftTable+"_"+leftCol+"_PK"))
													{
														join_rhs_local = index_scan_list.get(leftTable+"_"+leftCol+"_PK");
														// When use IndexScanNode we should reset the schema.
														
														///////////////////////////////////////////////////////////////////////////////////////////////////////////
													}
													else
													{
														join_rhs_local = new IndexScanNode(leftTable+"_"+leftCol+"_PK", tv.get(leftTable).coldef);
														index_scan_list.put(leftTable+"_"+leftCol+"_PK", join_rhs_local);
													}
													///////////////////////////////////////////////////////////////////////////////////////////////////////////////////
											
													joinNode = IndexLoopJoinNode.make(join_lhs_local, join_rhs_local, exp);
													if (oneTblCondition.containsKey(t1.toUpperCase()))
													{
														//Do select again.
														Expression sel_condition = oneTblCondition.get(t1.toUpperCase()).get(0);
														for (int i = 1; i < oneTblCondition.get(t1.toUpperCase()).size(); i++) {
															sel_condition = new AndExpression(sel_condition, oneTblCondition.get(t1.toUpperCase()).get(i));
														}
														joinNode = SelectionNode.make(joinNode, sel_condition, null, null);
													}
													//After IndexLoopJoin, we should modify the last part of its schema
													if (!t1.equalsIgnoreCase(leftTable)){
														//((IndexLoopJoinNode) joinNode).modifyLastPartSchema(tv.get(t1).coldef);
														((IndexLoopJoinNode) joinNode).setSchema(modifyJoinSchema(joinNode.getSchemaVars(), tv.get(t1).coldef));
													}
													//System.out.println("join on primary index of left table: " + exp.toString());
												}
												else if (indexStorage.containsKey(leftTable+"_"+leftCol+"_IDX")) {
													// join on secondary index of leftTable
													PlanNode join_lhs_local = joinNode;
													PlanNode join_rhs_local;
													if(index_scan_list.containsKey(leftTable+"_"+leftCol+"_IDX"))
													{
														join_rhs_local = index_scan_list.get(leftTable+"_"+leftCol+"_IDX");
													}
													else
													{
														join_rhs_local = new IndexScanNode(leftTable+"_"+leftCol+"_IDX", tv.get(leftTable).coldef);
														index_scan_list.put(leftTable+"_"+leftCol+"_IDX", join_rhs_local);
													} 

													joinNode = IndexLoopJoinNode.make(join_lhs_local, join_rhs_local, exp);
													if (oneTblCondition.containsKey(t1.toUpperCase()))
													{
														//Do select again.
														Expression sel_condition = oneTblCondition.get(t1.toUpperCase()).get(0);
														for (int i = 1; i < oneTblCondition.get(t1.toUpperCase()).size(); i++) {
															sel_condition = new AndExpression(sel_condition, oneTblCondition.get(t1.toUpperCase()).get(i));
														}
														joinNode = SelectionNode.make(joinNode, sel_condition, null, null);
													}
													//After IndexLoopJoin, we should modify the last part of its schema
													if (!t1.equalsIgnoreCase(leftTable)){
														//((IndexLoopJoinNode) joinNode).modifyLastPartSchema(tv.get(t1).coldef);
														((IndexLoopJoinNode) joinNode).setSchema(modifyJoinSchema(joinNode.getSchemaVars(), tv.get(t1).coldef));
													}
													//System.out.println("join on secondary index of left table: " + exp.toString());
												}
												else {
													joinNode = HashJoinNode.make(joinNode, scan_list.get(t1), exp, swapDir);
												}
												//joinNode = HashJoinNode.make(joinNode, scan_list.get(t1), exp, swapDir);
											}
									    }
								    } // end of condition check
									
									root_child = joinNode;
									if (sub_parenthesisExpression != null && sub_parenthesisExpression.size() > 0)
									{
										Expression sel_condition_p = sub_parenthesisExpression.get(0);
										String indexName = null;
										List<Expression> rangeExpression = new ArrayList<Expression>();										
										indexName = getIndexName(scan_tblName, sub_oneTblCondition.get(scan_tblName.toUpperCase()).get(0));
										if(indexName != null)
											rangeExpression.add(sub_oneTblCondition.get(scan_tblName.toUpperCase()).get(0));											
										for (int i = 1; i < sub_parenthesisExpression.size(); i++)
										{
											String idx = getIndexName(scan_tblName, sub_oneTblCondition.get(scan_tblName.toUpperCase()).get(i));										
											if(idx != null)
											{
												indexName = idx;
												rangeExpression.add(sub_oneTblCondition.get(scan_tblName.toUpperCase()).get(i));
											} 
											else
												sel_condition_p = new AndExpression(sel_condition_p, sub_parenthesisExpression.get(i));
										}
										root_child = SelectionNode.make(root_child, sel_condition_p, indexName, rangeExpression);
									}									
								}
								else
								{// only one table
									String scan_tblName = sub_select.getFromItem().toString().toUpperCase();
									root_child = SelectionNode.make(new BlockScanNode(new File(dataDir, scan_tblName + ".dat"), tv.get(scan_tblName.toUpperCase()).coldef), sub_select.getWhere());
								}
								//root_child = SelectionNode.make(root_child, sub_select.getWhere());
								
								boolean isAggr = false;
								for (int i = 0; i < sub_select.getSelectItems().size(); i++) {
									Expression expr1 = ((SelectExpressionItem) sub_select.getSelectItems().get(i)).getExpression();
									if (expr1 instanceof Function) {
										isAggr = true;
										break;
									}
								}
								if (isAggr)
								{
									root_child = AggregationNode.make(root_child, sub_select.getSelectItems(), sub_select.getGroupByColumnReferences(), sub_select.getOrderByElements());
								}
								else 
								{
									root_child = ProjectionNode.make(root_child, sub_select.getSelectItems());
								}
								if (sub_select.getOrderByElements() != null)
									root_child = SortNode.make(root, sub_select.getOrderByElements(), swapDir);
							}
							else
							{
								if (pselect.getJoins() != null)
								{
									PlanNode joinNode = null;
									HashMap<String, PlanNode> scan_list = new HashMap<String, PlanNode>();
									//For each table, only one IndexScanNode should be created.
									HashMap<String, PlanNode> index_scan_list = new HashMap<String, PlanNode>();
									//For IndexJoin, when using IndexScanNode as rhs, in case of tableName is alias, we have to get the original table name.
									HashMap<String, String> alias_name_map = new HashMap<String, String>();
									
									String scan_tblName = pselect.getFromItem().toString().toUpperCase();
									String alias = scan_tblName;
									List<ColumnDef> scan_schema = tv.get(scan_tblName).coldef;
									
									if (((Table)pselect.getFromItem()).getAlias() != null)
									{
										alias = ((Table)pselect.getFromItem()).getAlias().toUpperCase();
										List<ColumnDef> oldCol = tv.get(scan_tblName.toUpperCase()).getColumnDef();
										tv.put(alias, changeTableAlias(alias, oldCol) );
										scan_schema = tv.get(alias).coldef;
										alias_name_map.put(alias, scan_tblName);
									}
									scan_list.put(alias, new BlockScanNode(new File(dataDir, scan_tblName + ".dat"), scan_schema));
									
									if (oneTblCondition.containsKey(scan_tblName.toUpperCase()))
									{
										List<Expression> rangeExpression = new ArrayList<Expression>();
										String indexName = null;
										Expression sel_condition = oneTblCondition.get(scan_tblName.toUpperCase()).get(0);
										indexName = getIndexName(scan_tblName, oneTblCondition.get(scan_tblName.toUpperCase()).get(0));
										if(indexName != null)
											rangeExpression.add(oneTblCondition.get(scan_tblName.toUpperCase()).get(0));
										for (int i = 1; i < oneTblCondition.get(scan_tblName.toUpperCase()).size(); i ++)
										{
											String idx = getIndexName(scan_tblName, oneTblCondition.get(scan_tblName.toUpperCase()).get(i));
											if(idx != null)
											{
												indexName = idx;
												rangeExpression.add(oneTblCondition.get(scan_tblName.toUpperCase()).get(i));
											}
											else 
												sel_condition = new AndExpression(sel_condition, oneTblCondition.get(scan_tblName.toUpperCase()).get(i));
										}
										if(scan_list.containsKey(alias))
											scan_list.put(alias, SelectionNode.make(scan_list.get(alias), sel_condition, indexName, rangeExpression));
										else
											scan_list.put(alias, SelectionNode.make(new BlockScanNode(new File(dataDir, scan_tblName + ".dat"), scan_schema), sel_condition, indexName, rangeExpression));
									}
									else
									{
										if(scan_list.containsKey(alias))
											scan_list.put(alias, SelectionNode.make(scan_list.get(alias), null));
										else
											scan_list.put(alias, SelectionNode.make(new BlockScanNode(new File(dataDir, scan_tblName + ".dat"), scan_schema), null));
									}
																											
									for (Object e: pselect.getJoins())
									{
										if(e instanceof Join)
										{
											//Set alias
											scan_tblName = ((Table)((Join) e).getRightItem()).getName().toUpperCase();
											alias = scan_tblName;
											scan_schema = tv.get(scan_tblName).coldef;
											
											if (((Table)((Join) e).getRightItem()).getAlias() != null)
											{
												alias = ((Table)((Join) e).getRightItem()).getAlias().toUpperCase();
												List<ColumnDef> oldCol = tv.get(scan_tblName.toUpperCase()).getColumnDef();
												tv.put(alias, changeTableAlias(alias, oldCol) );
												scan_schema = tv.get(alias).coldef;
												alias_name_map.put(alias, scan_tblName);
											}
											scan_list.put(alias, new BlockScanNode(new File(dataDir, scan_tblName + ".dat"), scan_schema));
											
											if (oneTblCondition.containsKey(scan_tblName.toUpperCase()))
											{
												List<Expression> rangeExpression = new ArrayList<Expression>();
												String indexName = null;												
												Expression sel_condition = oneTblCondition.get(scan_tblName.toUpperCase()).get(0);
												indexName = getIndexName(scan_tblName, oneTblCondition.get(scan_tblName.toUpperCase()).get(0));
												if(indexName != null)
													rangeExpression.add(oneTblCondition.get(scan_tblName.toUpperCase()).get(0));						
												for (int i = 1; i < oneTblCondition.get(scan_tblName.toUpperCase()).size(); i ++)
												{
													String idx = getIndexName(scan_tblName, oneTblCondition.get(scan_tblName.toUpperCase()).get(i));
													if(idx != null)
													{
														indexName = idx;
														rangeExpression.add(oneTblCondition.get(scan_tblName.toUpperCase()).get(i));							
													}
													else
														sel_condition = new AndExpression(sel_condition, oneTblCondition.get(scan_tblName.toUpperCase()).get(i));													
												}
												if(scan_list.containsKey(alias))
													scan_list.put(alias, SelectionNode.make(scan_list.get(alias), sel_condition, indexName, rangeExpression));
												else
													scan_list.put(alias, SelectionNode.make(new BlockScanNode(new File(dataDir, scan_tblName + ".dat"), scan_schema), sel_condition, indexName, rangeExpression));
											}
											else
											{
												if(scan_list.containsKey(alias))
													scan_list.put(alias, SelectionNode.make(scan_list.get(alias), null));
												else
													scan_list.put(alias, SelectionNode.make(new BlockScanNode(new File(dataDir, scan_tblName + ".dat"), scan_schema), null));
											}									
										}																		
										
																				
									}

									Set<String> tableList = new HashSet<String>();
									List<Expression> expList = exprParser.getExpressionList();
									
									
									for (Expression exp : expList)
								    {
										if (((BinaryExpression) exp).getLeftExpression() instanceof Column
												&& ((BinaryExpression) exp).getRightExpression() instanceof Column) 
										{
								        
								        Expression leftExpr = ((BinaryExpression) exp).getLeftExpression();
										String t1 = ((Column)leftExpr).getTable().getName().toUpperCase();
										Expression rightExpr = ((BinaryExpression) exp).getRightExpression();
										String t2 = ((Column)rightExpr).getTable().getName().toUpperCase();
								        
										if(!tableList.contains(t1) && !tableList.contains(t2))
										{
											//first round of join
											tableList.add(t1);
											tableList.add(t2);
											// If there is an index present on join condition attribute in either left table or right table, then use IndexLoopJoin
											// Parse join condition
											String leftTable = ((Column) ((BinaryExpression) exp).getLeftExpression()).getTable().getName().toUpperCase();
											String rightTable = ((Column) ((BinaryExpression) exp).getRightExpression()).getTable().getName().toUpperCase();
											String leftCol = ((Column) ((BinaryExpression) exp).getLeftExpression()).getColumnName().toUpperCase();
											String rightCol = ((Column) ((BinaryExpression) exp).getRightExpression()).getColumnName().toUpperCase();
											// check whether left table or right table is an alias, if yes, then change it back to original name;
											if(alias_name_map.containsKey(leftTable))
												leftTable = alias_name_map.get(leftTable);
											if(alias_name_map.containsKey(rightTable))
												rightTable = alias_name_map.get(rightTable);
											if(indexStorage.containsKey(rightTable+"_"+rightCol+"_PK")) {
												//join on Primary key index of rightTable
												PlanNode join_lhs_local = scan_list.get(t1);
												//For first round of join.
												PlanNode join_rhs_local = new IndexScanNode(rightTable+"_"+rightCol+"_PK", tv.get(rightTable).coldef);
												index_scan_list.put(rightTable+"_"+rightCol+"_PK", join_rhs_local);
												joinNode = IndexLoopJoinNode.make(join_lhs_local, join_rhs_local, exp);
												//After index join, we may lose some selection conditions; here pick them up;
												if (oneTblCondition.containsKey(t2.toUpperCase()))
												{
													//Do select again.
													Expression sel_condition = oneTblCondition.get(t2.toUpperCase()).get(0);
													for (int i = 1; i < oneTblCondition.get(t2.toUpperCase()).size(); i++) {
														sel_condition = new AndExpression(sel_condition, oneTblCondition.get(t2.toUpperCase()).get(i));
													}
													joinNode = SelectionNode.make(joinNode, sel_condition, null, null);
												}
												//After IndexLoopJoin, we should modify the last part of its schema
												if (!t2.equalsIgnoreCase(rightTable)){
													((IndexLoopJoinNode) joinNode).modifyLastPartSchema(tv.get(t2).coldef);
												}
												
												//System.out.println("join on primary index of right table: " + exp.toString());
											}
											else if (indexStorage.containsKey(leftTable+"_"+leftCol+"_PK")) {
												// join on Primary key index of leftTable
												PlanNode join_lhs_local = scan_list.get(t2);
												PlanNode join_rhs_local = new IndexScanNode(leftTable+"_"+leftCol+"_PK", tv.get(leftTable).coldef);
												index_scan_list.put(leftTable+"_"+leftCol+"_PK", join_rhs_local);
												joinNode = IndexLoopJoinNode.make(join_lhs_local, join_rhs_local, exp);
												if (oneTblCondition.containsKey(t1.toUpperCase()))
												{
													//Do select again.
													Expression sel_condition = oneTblCondition.get(t1.toUpperCase()).get(0);
													for (int i = 1; i < oneTblCondition.get(t1.toUpperCase()).size(); i++) {
														sel_condition = new AndExpression(sel_condition, oneTblCondition.get(t1.toUpperCase()).get(i));
													}
													joinNode = SelectionNode.make(joinNode, sel_condition, null, null);
												}
												//After IndexLoopJoin, we should modify the last part of its schema
												if (!t1.equalsIgnoreCase(leftTable)){
													((IndexLoopJoinNode) joinNode).modifyLastPartSchema(tv.get(t1).coldef);
												}
												//System.out.println("join on primary index of left table: " + exp.toString());
											}
											else if (indexStorage.containsKey(rightTable+"_"+rightCol+"_IDX")) {
												// join on secondary index of rightTable
												PlanNode join_lhs_local = scan_list.get(t1);
												PlanNode join_rhs_local = new IndexScanNode(rightTable+"_"+rightCol+"_IDX", tv.get(rightTable).coldef);
												index_scan_list.put(rightTable+"_"+rightCol+"_IDX", join_rhs_local);
												joinNode = IndexLoopJoinNode.make(join_lhs_local, join_rhs_local, exp);
												//After index join, we may lose some selection conditions; here pick them up;
												if (oneTblCondition.containsKey(t2.toUpperCase()))
												{
													//Do select again.
													Expression sel_condition = oneTblCondition.get(t2.toUpperCase()).get(0);
													for (int i = 1; i < oneTblCondition.get(t2.toUpperCase()).size(); i++) {
														sel_condition = new AndExpression(sel_condition, oneTblCondition.get(t2.toUpperCase()).get(i));
													}
													joinNode = SelectionNode.make(joinNode, sel_condition, null, null);
												}
												//After IndexLoopJoin, we should modify the last part of its schema
												if (!t2.equalsIgnoreCase(rightTable)){
													((IndexLoopJoinNode) joinNode).modifyLastPartSchema(tv.get(t2).coldef);
												}
												//System.out.println("join on secondary index of right table: " + exp.toString());
											}
											else if (indexStorage.containsKey(leftTable+"_"+leftCol+"_IDX")) {
												// join on secondary index of leftTable
												PlanNode join_lhs_local = scan_list.get(t2);
												PlanNode join_rhs_local = new IndexScanNode(leftTable+"_"+leftCol+"_IDX", tv.get(leftTable).coldef);
												index_scan_list.put(leftTable+"_"+leftCol+"_IDX", join_rhs_local);
												joinNode = IndexLoopJoinNode.make(join_lhs_local, join_rhs_local, exp);
												if (oneTblCondition.containsKey(t1.toUpperCase()))
												{
													//Do select again.
													Expression sel_condition = oneTblCondition.get(t1.toUpperCase()).get(0);
													for (int i = 1; i < oneTblCondition.get(t1.toUpperCase()).size(); i++) {
														sel_condition = new AndExpression(sel_condition, oneTblCondition.get(t1.toUpperCase()).get(i));
													}
													joinNode = SelectionNode.make(joinNode, sel_condition, null, null);
												}
												//After IndexLoopJoin, we should modify the last part of its schema
												if (!t1.equalsIgnoreCase(leftTable)){
													((IndexLoopJoinNode) joinNode).modifyLastPartSchema(tv.get(t1).coldef);
												}
												//System.out.println("join on secondary index of left table: " + exp.toString());
											}
											else {
												joinNode = HashJoinNode.make(scan_list.get(t1), scan_list.get(t2), exp, swapDir);
											}
											//joinNode = HashJoinNode.make(scan_list.get(t1), scan_list.get(t2), exp, swapDir);
										}
										else if(tableList.contains(t1))
										{
											tableList.add(t2);
											// If rhs of join expression has an index on join attribute, then use indexLoopJoin
											String leftTable = ((Column) ((BinaryExpression) exp).getLeftExpression()).getTable().getName().toUpperCase();
											String rightTable = ((Column) ((BinaryExpression) exp).getRightExpression()).getTable().getName().toUpperCase();
											String leftCol = ((Column) ((BinaryExpression) exp).getLeftExpression()).getColumnName().toUpperCase();
											String rightCol = ((Column) ((BinaryExpression) exp).getRightExpression()).getColumnName().toUpperCase();
											// check whether left table or right table is an alias, if yes, then change it back to original name;
											if(alias_name_map.containsKey(leftTable))
												leftTable = alias_name_map.get(leftTable);
											if(alias_name_map.containsKey(rightTable))
												rightTable = alias_name_map.get(rightTable);
											if(indexStorage.containsKey(rightTable+"_"+rightCol+"_PK")) {
												//join on Primary key index of rightTable
												PlanNode join_lhs_local = joinNode;
												//If IndexScanNode is already there, should new again. But They original one.
												PlanNode join_rhs_local;
												if(index_scan_list.containsKey(rightTable+"_"+rightCol+"_PK"))
												{
													join_rhs_local = index_scan_list.get(rightTable+"_"+rightCol+"_PK");
													// When use IndexScanNode we should reset the schema.
													
													///////////////////////////////////////////////////////////////////////////////////////////////////////////
												}
												else
												{
													join_rhs_local = new IndexScanNode(rightTable+"_"+rightCol+"_PK", tv.get(rightTable).coldef);
													index_scan_list.put(rightTable+"_"+rightCol+"_PK", join_rhs_local);
												}
												///////////////////////////////////////////////////////////////////////////////////////////////////////////////////
										
												joinNode = IndexLoopJoinNode.make(join_lhs_local, join_rhs_local, exp);
												//After index join, we may lose some selection conditions; here pick them up;
												if (oneTblCondition.containsKey(t2.toUpperCase()))
												{
													//Do select again.
													Expression sel_condition = oneTblCondition.get(t2.toUpperCase()).get(0);
													for (int i = 1; i < oneTblCondition.get(t2.toUpperCase()).size(); i++) {
														sel_condition = new AndExpression(sel_condition, oneTblCondition.get(t2.toUpperCase()).get(i));
													}
													joinNode = SelectionNode.make(joinNode, sel_condition, null, null);
												}
												//After IndexLoopJoin, we should modify the last part of its schema
												if (!t2.equalsIgnoreCase(rightTable)){
													//////////////////////////////////////////////////////////////////
													((IndexLoopJoinNode) joinNode).setSchema(modifyJoinSchema(joinNode.getSchemaVars(), tv.get(t2).coldef));
													//((IndexLoopJoinNode) joinNode).modifyLastPartSchema(tv.get(t2).coldef);
													
												}
												//System.out.println("join on primary index of right table: " + exp.toString());
											}
											else if (indexStorage.containsKey(rightTable+"_"+rightCol+"_IDX")) {
												// join on secondary index of rightTable
												PlanNode join_lhs_local = joinNode;
												PlanNode join_rhs_local;
												if(index_scan_list.containsKey(rightTable+"_"+rightCol+"_IDX"))
												{
													join_rhs_local = index_scan_list.get(rightTable+"_"+rightCol+"_IDX");
												}
												else
												{
													join_rhs_local = new IndexScanNode(rightTable+"_"+rightCol+"_IDX", tv.get(rightTable).coldef);
													index_scan_list.put(rightTable+"_"+rightCol+"_IDX", join_rhs_local);
												} 

												joinNode = IndexLoopJoinNode.make(join_lhs_local, join_rhs_local, exp);
												//After index join, we may lose some selection conditions; here pick them up;
												if (oneTblCondition.containsKey(t2.toUpperCase()))
												{
													//Do select again.
													Expression sel_condition = oneTblCondition.get(t2.toUpperCase()).get(0);
													for (int i = 1; i < oneTblCondition.get(t2.toUpperCase()).size(); i++) {
														sel_condition = new AndExpression(sel_condition, oneTblCondition.get(t2.toUpperCase()).get(i));
													}
													joinNode = SelectionNode.make(joinNode, sel_condition, null, null);
												}
												//After IndexLoopJoin, we should modify the last part of its schema
												if (!t2.equalsIgnoreCase(rightTable)){
													((IndexLoopJoinNode) joinNode).modifyLastPartSchema(tv.get(t2).coldef);
												}
												//System.out.println("join on secondary index of right table: " + exp.toString());
											}
											else {
												joinNode = HashJoinNode.make(joinNode, scan_list.get(t2), exp, swapDir);
											}
											//joinNode = HashJoinNode.make(joinNode, scan_list.get(t2), exp, swapDir);
											
										}
										else if(tableList.contains(t2))
										{
											tableList.add(t1);
											// If lhs of join expression has an index on join attribute, then use indexLoopJoin
											String leftTable = ((Column) ((BinaryExpression) exp).getLeftExpression()).getTable().getName().toUpperCase();
											String rightTable = ((Column) ((BinaryExpression) exp).getRightExpression()).getTable().getName().toUpperCase();
											String leftCol = ((Column) ((BinaryExpression) exp).getLeftExpression()).getColumnName().toUpperCase();
											String rightCol = ((Column) ((BinaryExpression) exp).getRightExpression()).getColumnName().toUpperCase();
											// check whether left table or right table is an alias, if yes, then change it back to original name;
											if(alias_name_map.containsKey(leftTable))
												leftTable = alias_name_map.get(leftTable);
											if(alias_name_map.containsKey(rightTable))
												rightTable = alias_name_map.get(rightTable);
											if(indexStorage.containsKey(leftTable+"_"+leftCol+"_PK")) {
												//join on Primary key index of leftTable
												PlanNode join_lhs_local = joinNode;
												//If IndexScanNode is already there, should new again. But They original one.
												PlanNode join_rhs_local;
												if(index_scan_list.containsKey(leftTable+"_"+leftCol+"_PK"))
												{
													join_rhs_local = index_scan_list.get(leftTable+"_"+leftCol+"_PK");
													// When use IndexScanNode we should reset the schema.
													
													///////////////////////////////////////////////////////////////////////////////////////////////////////////
												}
												else
												{
													join_rhs_local = new IndexScanNode(leftTable+"_"+leftCol+"_PK", tv.get(leftTable).coldef);
													index_scan_list.put(leftTable+"_"+leftCol+"_PK", join_rhs_local);
												}
												///////////////////////////////////////////////////////////////////////////////////////////////////////////////////
										
												joinNode = IndexLoopJoinNode.make(join_lhs_local, join_rhs_local, exp);
												if (oneTblCondition.containsKey(t1.toUpperCase()))
												{
													//Do select again.
													Expression sel_condition = oneTblCondition.get(t1.toUpperCase()).get(0);
													for (int i = 1; i < oneTblCondition.get(t1.toUpperCase()).size(); i++) {
														sel_condition = new AndExpression(sel_condition, oneTblCondition.get(t1.toUpperCase()).get(i));
													}
													joinNode = SelectionNode.make(joinNode, sel_condition, null, null);
												}
												//After IndexLoopJoin, we should modify the last part of its schema
												if (!t1.equalsIgnoreCase(leftTable)){
													((IndexLoopJoinNode) joinNode).modifyLastPartSchema(tv.get(t1).coldef);
												}
												//System.out.println("join on primary index of left table: " + exp.toString());
											}
											else if (indexStorage.containsKey(leftTable+"_"+leftCol+"_IDX")) {
												// join on secondary index of leftTable
												PlanNode join_lhs_local = joinNode;
												PlanNode join_rhs_local;
												if(index_scan_list.containsKey(leftTable+"_"+leftCol+"_IDX"))
												{
													join_rhs_local = index_scan_list.get(leftTable+"_"+leftCol+"_IDX");
												}
												else
												{
													join_rhs_local = new IndexScanNode(leftTable+"_"+leftCol+"_IDX", tv.get(leftTable).coldef);
													index_scan_list.put(leftTable+"_"+leftCol+"_IDX", join_rhs_local);
												} 

												joinNode = IndexLoopJoinNode.make(join_lhs_local, join_rhs_local, exp);
												if (oneTblCondition.containsKey(t1.toUpperCase()))
												{
													//Do select again.
													Expression sel_condition = oneTblCondition.get(t1.toUpperCase()).get(0);
													for (int i = 1; i < oneTblCondition.get(t1.toUpperCase()).size(); i++) {
														sel_condition = new AndExpression(sel_condition, oneTblCondition.get(t1.toUpperCase()).get(i));
													}
													joinNode = SelectionNode.make(joinNode, sel_condition, null, null);
												}
												//After IndexLoopJoin, we should modify the last part of its schema
												if (!t1.equalsIgnoreCase(leftTable)){
													((IndexLoopJoinNode) joinNode).modifyLastPartSchema(tv.get(t1).coldef);
												}
												//System.out.println("join on secondary index of left table: " + exp.toString());
											}
											else {
												joinNode = HashJoinNode.make(joinNode, scan_list.get(t1), exp, swapDir);
											}
											//joinNode = HashJoinNode.make(joinNode, scan_list.get(t1), exp, swapDir);
										}
										}
								    } // end of condition check
									
									root_child = joinNode;
									if (parenthesisExpression != null & parenthesisExpression.size() > 0)
									{
										Expression sel_condition_p = parenthesisExpression.get(0);
										for (int i = 1; i < parenthesisExpression.size(); i ++)
										{
											sel_condition_p = new AndExpression(sel_condition_p, parenthesisExpression.get(i));
										}
										root_child = SelectionNode.make(root_child, sel_condition_p);
									}
								}
								else  // only 1 table
								{
									String scan_tblName = pselect.getFromItem().toString().toUpperCase();
									String alias = scan_tblName;
									List<ColumnDef> scan_schema = tv.get(scan_tblName).coldef;
									
									if (((Table)pselect.getFromItem()).getAlias() != null)
									{
										alias = ((Table)pselect.getFromItem()).getAlias().toUpperCase();
										List<ColumnDef> oldCol = tv.get(scan_tblName.toUpperCase()).getColumnDef();
										tv.put(alias, changeTableAlias(alias, oldCol) );
										scan_schema = tv.get(alias).coldef;
									}
								
									if (oneTblCondition.containsKey(scan_tblName.toUpperCase()))
									{
										List<Expression> rangeExpression = new ArrayList<Expression>();
										String indexName = null;
										Expression sel_condition = oneTblCondition.get(scan_tblName.toUpperCase()).get(0);
										indexName = getIndexName(scan_tblName, oneTblCondition.get(scan_tblName.toUpperCase()).get(0));
										if(indexName != null)
											rangeExpression.add(oneTblCondition.get(scan_tblName.toUpperCase()).get(0));										
										for (int i = 1; i < oneTblCondition.get(scan_tblName.toUpperCase()).size(); i ++)
										{
											String idx = getIndexName(scan_tblName, oneTblCondition.get(scan_tblName.toUpperCase()).get(i));
											if(idx != null)
											{
												indexName = idx;
												rangeExpression.add(oneTblCondition.get(scan_tblName.toUpperCase()).get(i));
											}
											else
												sel_condition = new AndExpression(sel_condition, oneTblCondition.get(scan_tblName.toUpperCase()).get(i));
										}
										root_child = SelectionNode.make(new BlockScanNode(new File(dataDir, scan_tblName + ".dat"), scan_schema), sel_condition, indexName, rangeExpression);
									}
									else
									{
										root_child = SelectionNode.make(new BlockScanNode(new File(dataDir, scan_tblName + ".dat"), scan_schema), pselect.getWhere());										
									}									
								}
							}
							boolean isAggr = false;
							for (int i = 0; i < pselect.getSelectItems().size(); i++) {
								Expression expr1 = ((SelectExpressionItem) pselect.getSelectItems().get(i)).getExpression();
								if (expr1 instanceof Function) {
									isAggr = true;
									break;
								}
							}
							if (isAggr)
							{
								root = AggregationNode.make(root_child, pselect.getSelectItems(), pselect.getGroupByColumnReferences(), pselect.getOrderByElements());
							}
							else 
							{
								root = ProjectionNode.make(root_child, pselect.getSelectItems());
							}
							if (pselect.getOrderByElements() != null)
								root = SortNode.make(root, pselect.getOrderByElements(), swapDir);
							
							BufferedWriter buffw = null;
							File f;
							try 
							{
							f = File.createTempFile("FinalResult_", "", swapDir);
							buffw = new BufferedWriter(new FileWriter(f));
							
							Datum[] row = root.readOneTuple();
							if (pselect.getLimit() != null)
							{
								int limit = (int)pselect.getLimit().getRowCount();
								for (int j = 0; j < limit && row!=null; j++)
								{
									for (int i = 0; i < row.length; i++)
									{
										System.out.print(row[i].toString());
										if(i < row.length - 1)
											System.out.print("|");				
									}						
									System.out.println("");
									
									row = root.readOneTuple();
								}
							}
							else
							{								
								while(row != null)
								{			
									for (int i = 0; i < row.length; i++)
									{
										System.out.print(row[i].toString());
										if(i < row.length - 1)
											System.out.print("|");				
									}						
									System.out.println("");
									row = root.readOneTuple();
								}
							}
							buffw.close();
						} catch (IOException e) 
						{
							try {
								buffw.close();
							} catch (IOException e1) {
								e1.printStackTrace();
							}
							e.printStackTrace();
						}		
														
						} else
							System.out.println("UNIONS NOT HANDLED");
					}
				}
			} catch (FileNotFoundException fnfe) {
				fnfe.printStackTrace();
			} catch (ParseException e) {
				e.printStackTrace();
			}
		}
		long time_end = System.currentTimeMillis();
		//System.out.println("Time Spent: " + (time_end - time_start) / 1000.0);
	}
	
	private static TableView changeTableAlias(String als, List<ColumnDef> oldCol)
	{
		TableView tView = new TableView();
		
		List<ColumnDef> newCol = new ArrayList<ColumnDef>();
		for (int j = 0; j < oldCol.size(); j++)
		{
			ColumnDef c = new ColumnDef(oldCol.get(j));
			c.setTableName(als);
			newCol.add(c);
		}	
		tView.setColDef(newCol);
		return tView;
		
	}
	
	public static List<ColumnDef> modifyJoinSchema(List<ColumnDef> join_schema, List<ColumnDef> rhs_schema) 
	{
		List<ColumnDef> newSchema = new ArrayList<ColumnDef>();
		for (int i = 0; i < join_schema.size() - rhs_schema.size(); i ++)
			newSchema.add(join_schema.get(i));
		for (int i = 0; i < rhs_schema.size(); i++)
			newSchema.add(rhs_schema.get(i));
		return newSchema;
		
	}
	
	// If you want to run the code without using indexes, comment everything in this function and return null
	private static String getIndexName(String tableName, Expression exp)
	{
		if((((BinaryExpression) exp).getLeftExpression() instanceof Column && 
				((BinaryExpression) exp).getRightExpression() instanceof Column) || exp instanceof Parenthesis)
		return null;
		
		String colName = ((Column) ((BinaryExpression)exp).getLeftExpression()).getColumnName();
		String indexName = (tableName + "_" + colName).toUpperCase() + "_";
		if(indexStorage.containsKey(indexName+"IDX"))
		{
			indexName += "IDX";
		}
		else if(indexStorage.containsKey(indexName+"PK"))
		{
			indexName += "PK";
		}
		else
			indexName = null;
		return indexName;
		//return null;
	}
}