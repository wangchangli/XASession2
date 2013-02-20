package com.xingcloud.xa.session2.ra.impl;

import com.xingcloud.xa.session2.ra.*;
import com.xingcloud.xa.session2.ra.expr.Expression;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Author: mulisen
 * Date:   2/6/13
 */
public class XSelection extends AbstractOperation implements Selection{

	RelationProvider relation;
	Expression expression;

	public Selection setInput(RelationProvider relation, Expression e) {
		resetInput();
		this.relation = relation;
		this.expression = e;
		addInput(relation);
		return this;
	}

	public Relation evaluate(){
        if (result == null){
            //return null;  //TODO method implementation
            List<Object[]> rows = new ArrayList<Object[]>();
            Map<String, Integer> columnIndex = new TreeMap<String, Integer>();

            XRelation.XRow row = null;
            while ((row = (XRelation.XRow)relation.nextRow()) != null){
                if (expression == null || ((Boolean)expression.evaluate(row))){
                    rows.add(row.rowData);
                    columnIndex = row.columnNames;
                }
            }
            result = new XRelation(columnIndex, rows);
        }

        return  result;
	}

	@Override
	public String toString() {
		return IndentPrint.print(this);
	}
}
