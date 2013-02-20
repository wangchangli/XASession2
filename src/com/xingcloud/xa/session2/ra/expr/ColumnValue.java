package com.xingcloud.xa.session2.ra.expr;

import com.xingcloud.xa.session2.ra.Row;

/**
 * Author: mulisen
 * Date:   2/6/13
 */
public class ColumnValue implements Expression {

	public String columnName;

	public ColumnValue(String columnName) {
		this.columnName = columnName;
	}

	public Object evaluate(Row input){
        //System.out.println("sssss"+columnName);
		return input.get(columnName);
	}
}
