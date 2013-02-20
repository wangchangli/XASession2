package com.xingcloud.xa.session2.ra.impl;

import com.xingcloud.xa.session2.ra.Aggregation;
import com.xingcloud.xa.session2.ra.RelationProvider;
import com.xingcloud.xa.session2.ra.Row;
import com.xingcloud.xa.session2.ra.Sum;

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
        //if (result == null){
        Row row = null;
        Long sum = 0L;
        while ((row=relation.nextRow()) != null){
            System.out.println(row.get(columnName));
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
