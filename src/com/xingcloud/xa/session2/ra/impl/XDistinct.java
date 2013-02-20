package com.xingcloud.xa.session2.ra.impl;

import com.xingcloud.xa.session2.ra.Distinct;
import com.xingcloud.xa.session2.ra.Relation;
import com.xingcloud.xa.session2.ra.RelationProvider;
import com.xingcloud.xa.session2.ra.Row;
import com.xingcloud.xa.session2.ra.expr.Expression;

import java.util.*;

/**
 * Author: mulisen
 * Date:   2/6/13
 */
public class XDistinct extends AbstractOperation implements Distinct {

	RelationProvider relation;
	Expression[] expressions;

	public Relation evaluate() {
		//return null;  //TODO method implementation
        if (result == null){
           List<Object[]> rows = new ArrayList<Object[]>();
            Map<String, Integer> columnIndex = new TreeMap<String, Integer>();

            XRelation.XRow row = null;
            Map<Expression, HashSet<String>> distinctMap = new HashMap<Expression, HashSet<String>>();

            while ((row = (XRelation.XRow)relation.nextRow()) != null){
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

	@Override
	public String toString() {
		return IndentPrint.print(this);
	}

}
