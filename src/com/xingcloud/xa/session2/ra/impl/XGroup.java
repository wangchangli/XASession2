package com.xingcloud.xa.session2.ra.impl;

import com.xingcloud.xa.session2.ra.*;
import com.xingcloud.xa.session2.ra.expr.Expression;

import java.util.*;

/**
 * Author: mulisen
 * Date:   2/7/13
 */
public class XGroup extends AbstractOperation implements Group {


	RelationProvider relation;

	Expression[] groupingExpressions;

	Expression[] projectionExpressions;

	public Relation evaluate() {
		//return null;  //TODO method implementation

        // group rows
        Map<String, List<Row>> groupRows = new HashMap<String, List<Row>>();
        Map<String, Integer> _columnIndex = new TreeMap<String, Integer>();
        RowIterator iterator = relation.iterator();
        while (iterator.hasNext()){
            XRelation.XRow row = (XRelation.XRow)iterator.nextRow();
            _columnIndex = row.columnNames;
            String groupString = "";
            for(Expression expression:groupingExpressions){
                groupString += (String)expression.evaluate(row);
            }

            if (!groupRows.containsKey(groupString)){
                groupRows.put(groupString,new ArrayList<Row>());
            }

            groupRows.get(groupString).add(row);
        }

        // projection for each different group
        Map<String, List<Object[]>> groupProjectionRows = new HashMap<String, List<Object[]>>();
        for(Map.Entry<String, List<Row>> entry:groupRows.entrySet()){
            List<Row> oldRows = entry.getValue();
            String groupString = entry.getKey();
            groupProjectionRows.put(groupString, new ArrayList<Object[]>());


            //construct xprojection for each group rows
            List<Object[]> _oldRows  = new ArrayList<Object[]>();
            for(Row row:oldRows){
                _oldRows.add(((XRelation.XRow)row).rowData);
            }
            XProjection xProjection = new XProjection();
            xProjection.setInput(new XRelation(_columnIndex,_oldRows),projectionExpressions);

            //projection
            XRelation relation = (XRelation)xProjection.evaluate();

            groupProjectionRows.get(groupString).addAll(relation.rows);

//            for(Row oldRow:oldRows){
//                Object[] newRow = new Object[projectionExpressions.length];
//                for (int i = 0; i < projectionExpressions.length; i++) {
//                    Expression proj = projectionExpressions[i];
//                    newRow[i] = proj.evaluate(oldRow);
//                }
//
//                groupProjectionRows.get(groupString).add(newRow);
//            }
        }

        // combine each group result
        List<Object[]> rows = new ArrayList<Object[]>();
        Map<String, Integer> columnIndex = new TreeMap<String, Integer>();
        for (int i = 0; i < projectionExpressions.length; i++) {
            Expression proj = projectionExpressions[i];
            StringBuilder sb = new StringBuilder();
            InlinePrint.printExpression(proj,sb);
            columnIndex.put(sb.toString(),i);
        }

        for(Map.Entry<String, List<Object[]>> entry:groupProjectionRows.entrySet()){
            rows.addAll(entry.getValue());
        }
        return new XRelation(columnIndex,rows);
	}

    public Group setInput(RelationProvider relation, Expression[] groupingExpressions, Expression[] projectionExpressions) {
		resetInput();
        this.relation = relation;
		this.groupingExpressions = groupingExpressions;
		this.projectionExpressions = projectionExpressions;
		addInput(relation);
        return this;
    }

	@Override
	public String toString() {
		return IndentPrint.print(this);
	}

}
