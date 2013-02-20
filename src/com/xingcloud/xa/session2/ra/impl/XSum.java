package com.xingcloud.xa.session2.ra.impl;

import com.xingcloud.xa.session2.ra.*;

/**
 * Author: mulisen
 * Date:   2/7/13
 */
public class XSum extends AbstractAggregation implements Sum {

	RelationProvider relation;
	String columnName;
	public Aggregation setInput(RelationProvider relation, String columnName) {
        resetInput();
		init();
		this.relation = relation;
		this.columnName = columnName;
		addInput(relation);
		return this;
	}

	public Object aggregate() {
		//return null;  //TODO method implementation
        //if (result == null){ //todo
        Long sum = 0L;
        RowIterator iterator = relation.iterator();
        while (iterator.hasNext()){
            Row row = iterator.nextRow();
            //System.out.println(row.get(columnName));
            sum+=Long.valueOf((String)row.get(columnName));
        }

        return String.valueOf(sum);
        //}

        //return  result;
	}

	public void init() {
		//TODO method implementation
	}

}
