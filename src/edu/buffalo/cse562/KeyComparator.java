package edu.buffalo.cse562;

import java.io.Serializable;
import java.util.Comparator;

import edu.buffalo.cse562.IndexRow;


class KeyComparator implements Serializable, Comparator<IndexRow>
{
	private static final long serialVersionUID = 1L;

	@Override
	public int compare(IndexRow row1, IndexRow row2) {
		return row1.compareTo(row2);
	}
}
	