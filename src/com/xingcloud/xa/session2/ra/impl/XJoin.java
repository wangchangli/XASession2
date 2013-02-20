package com.xingcloud.xa.session2.ra.impl;

import com.xingcloud.xa.session2.ra.*;
import com.xingcloud.xa.session2.ra.expr.Expression;

import java.security.KeyPair;
import java.util.*;

/**
 * Author: mulisen
 * Date:   2/6/13
 */
public class XJoin extends AbstractOperation implements Join{

	RelationProvider left;
	RelationProvider right;

	public Relation evaluate() {
		//return null;  //TODO method implementation

        List<Object[]> rows = new ArrayList<Object[]>();

        XRelation xLeft = (XRelation)((Operation)left).evaluate();
        XRelation xRight = (XRelation)((Operation)right).evaluate();

        List<Object[]> leftRows = xLeft.rows;
        List<Object[]> rightRows = xRight.rows;

        Map<String, Integer> leftColumnIndex = xLeft.columnIndex;
        Map<String, Integer> rightColumnIndex = xRight.columnIndex;


        Map<String, Integer> newColumnIndex = leftColumnIndex;  // todo
        List<String> sameCols = new ArrayList<String>();
        for (Map.Entry<String, Integer> lEntry: leftColumnIndex.entrySet()){
            for (Map.Entry<String, Integer> rEntry: rightColumnIndex.entrySet()){
                if (lEntry.getKey().equals(rEntry.getKey())){
                    sameCols.add(lEntry.getKey());
                }
            }
        }
        for(Object[] leftRow: leftRows){
            for(Object[] rightRow:rightRows){
                Boolean natural = true;
                for(String sameCol: sameCols){
                    if (!(leftRow[leftColumnIndex.get(sameCol)].toString().equals(rightRow[rightColumnIndex.get(sameCol)].toString()))){
                        natural = false;
                    }
                }
                if(natural){
                    Object[] newRow = new Object[leftRow.length+rightRow.length];
                    System.arraycopy(leftRow,0,newRow,0,leftRow.length);
                    System.arraycopy(rightRow,0,newRow,leftRow.length,rightRow.length);
                    rows.add(newRow);
                }
            }
        }

        return new XRelation(newColumnIndex, rows);
	}

	public Join setInput(RelationProvider left, RelationProvider right) {
		resetInput();
        this.left = left;
		this.right = right;
		addInput(left);
		addInput(right);
		return this;
	}

	@Override
	public String toString() {
		return IndentPrint.print(this);
	}

}
