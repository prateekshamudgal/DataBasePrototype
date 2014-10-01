package edu.buffalo.cse562;

import java.util.List;

import edu.buffalo.cse562.ColumnDef;
import edu.buffalo.cse562.IndexSerializer;


public class IndexAttributes
{
	String indexName;
	String indexType;
	List<ColumnDef> keySchema;
	List<Integer> keyColumns;
	IndexSerializer keySerializer;
	
	public IndexAttributes() {	}
	
	public String getIndexName() {
		return indexName;
	}
	
	public void setIndexName(String name) {
		this.indexName = name;
	}
	
	public String getIndexType() {
		return indexType;
	}
	
	public void setIndexType(String type) {
		this.indexType = type;
	}
	
	public List<ColumnDef> getKeySchema() {
		return keySchema;
	}
	
	public void setKeySchema(List<ColumnDef> keySchema) {
		this.keySchema = keySchema;
	}
	
	public IndexSerializer getKeySerializer() {
		return keySerializer;
	}
	
	public void setKeySerializer(IndexSerializer keySerializer) {
		this.keySerializer = keySerializer;
	}
	
	public List<Integer> getKeyColumns() {
		return keyColumns;
	}
	
	public void setKeyColumns(List<Integer> keyColumns) {
		this.keyColumns = keyColumns;
	}
}
