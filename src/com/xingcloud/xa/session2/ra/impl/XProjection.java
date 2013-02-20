package com.xingcloud.xa.session2.ra.impl;

import com.sun.org.apache.xml.internal.utils.res.XResources_en;
import com.xingcloud.xa.session2.ra.*;
import com.xingcloud.xa.session2.ra.expr.ColumnValue;
import com.xingcloud.xa.session2.ra.expr.Expression;
import com.xingcloud.xa.session2.ra.expr.AggregationExpr;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Author: mulisen
 * Date:   2/6/13
 */
public class XProjection extends AbstractOperation implements Projection{

	RelationProvider relation;
	Expression[] projections;

	public Relation evaluate() {
		//return null;  //TODO method implementation

        List<Object[]> rows = new ArrayList<Object[]>();
        Map<String, Integer> newColumnIndex = new TreeMap<String, Integer>();
        Map<String, Integer> oldColumnIndex = new TreeMap<String, Integer>();

        List<Expression> newProjections = new ArrayList<Expression>();

        XRelation xRelation = (XRelation)((Operation)relation).evaluate();
        oldColumnIndex = xRelation.columnIndex;

        // column(*)
        int colNum=0;
        for (int i = 0; i < projections.length; i++) {
            Expression proj = projections[i];
            if ((proj instanceof ColumnValue) && (((ColumnValue) proj).columnName.equals("*"))){
                for(Map.Entry<String, Integer> entry: oldColumnIndex.entrySet()){
                    Expression expression = new ColumnValue(entry.getKey());
                    newProjections.add(expression);
                    StringBuilder sb = new StringBuilder();
                    InlinePrint.printExpression(expression,sb);
                    newColumnIndex.put(sb.toString(), colNum);
                    colNum++;
                }
            }else{
                newProjections.add(proj);
                StringBuilder sb = new StringBuilder();
                InlinePrint.printExpression(proj,sb);
                newColumnIndex.put(sb.toString(), colNum); // todo toString impl
                colNum++;
            }
        }
//        for(Expression expression:newProjections){
//            System.out.println(((ColumnValue)expression).columnName);
//        }

        boolean hasAggregation = false;
        if(projections == null){
            //columnIndex = relation
        }else{
            RowIterator iterator = relation.iterator();
            Row oldRow = iterator.nextRow(); // just need one row
            Object[] newRow = new Object[newProjections.size()];
            for (int i = 0; i < newProjections.size(); i++) {
                Expression proj = newProjections.get(i);
                if (proj instanceof AggregationExpr){
                    hasAggregation = true;
                }
                newRow[i] = proj.evaluate(oldRow);
            }
            rows.add(newRow);
        }

        if (! hasAggregation){ //calculate each row
            RowIterator iterator = relation.iterator();
            iterator.nextRow();// we have already iterator one row
            while (iterator.hasNext()){
                Row oldRow = iterator.nextRow();
                Object[] newRow = new Object[newProjections.size()];
                for (int i = 0; i < newProjections.size(); i++) {
                    Expression proj = newProjections.get(i);
                    newRow[i] = proj.evaluate(oldRow);
                }
                rows.add(newRow);
            };
        }

        return new XRelation(newColumnIndex, rows);
	}

	public Projection setInput(RelationProvider relation, Expression ... projections) {
        resetInput();
		this.relation = relation;
		this.projections = projections;
		addInput(relation);
		return this;
	}

	@Override
	public String toString() {
		return IndentPrint.print(this);
	}
}
