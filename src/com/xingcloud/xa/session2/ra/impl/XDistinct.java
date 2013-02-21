package com.xingcloud.xa.session2.ra.impl;

import com.xingcloud.xa.session2.ra.*;
import com.xingcloud.xa.session2.ra.expr.Expression;

import java.util.*;

/**
 * Author: mulisen
 * Date:   2/6/13
 */
public class XDistinct extends AbstractOperation implements Distinct {

	RelationProvider relation;
	public Expression[] expressions;

	public Relation evaluate() {
		//return null;  //TODO method implementation
        if (result == null){
           List<Object[]> rows = new ArrayList<Object[]>();
            Map<String, Integer> columnIndex = new TreeMap<String, Integer>();

            Map<Expression, HashSet<String>> distinctMap = new HashMap<Expression, HashSet<String>>();

            RowIterator iterator = relation.iterator();
            while (iterator.hasNext()){
                XRelation.XRow row = (XRelation.XRow)iterator.nextRow();
                columnIndex = row.columnNames;   // todo
                Boolean distinct = true;
                for (int i = 0; i < expressions.length; i++){
                    Expression expression = expressions[i];

                    if (distinctMap.get(expression) == null){
                        distinctMap.put(expression, new HashSet<String>());
                    }

                    if (distinctMap.get(expression).contains(expression.evaluate(row).toString())){
                        distinct = false;
                        break;
                    }
                }
                if (distinct){
                    rows.add(row.rowData);
                    for (int i = 0; i < expressions.length; i++){
                        Expression expression = expressions[i];
                        distinctMap.get(expression).add(expression.evaluate(row).toString());
                    }
                }
            }

            result = new XRelation(columnIndex, rows);
        }

        return result;
	}

	public Distinct setInput(RelationProvider relation, Expression ... expressions ) {
		resetInput();
        this.relation = relation;
		this.expressions = expressions;
		addInput(relation);
		return this;
	}

    public void updateRelation(RelationProvider relation){
        this.relation = relation;
    }

	@Override
	public String toString() {
		return IndentPrint.print(this);
	}

}
