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
        if (result == null){
            List<Object[]> rows = new ArrayList<Object[]>();

            XRelation xLeft = (XRelation)((Operation)left).evaluate();
            XRelation xRight = (XRelation)((Operation)right).evaluate();

            List<Object[]> leftRows = xLeft.rows;
            List<Object[]> rightRows = xRight.rows;

            Map<String, Integer> leftColumnIndex = xLeft.columnIndex;
            Map<String, Integer> rightColumnIndex = xRight.columnIndex;


            // get the columns with the same name
            List<String> sameCols = new ArrayList<String>();
            List<Integer> rightSameColsIndex = new ArrayList<Integer>();
            for (Map.Entry<String, Integer> lEntry: leftColumnIndex.entrySet()){
                for (Map.Entry<String, Integer> rEntry: rightColumnIndex.entrySet()){
                    if (lEntry.getKey().equals(rEntry.getKey())){
                        sameCols.add(lEntry.getKey());
                        rightSameColsIndex.add(rEntry.getValue());
                    }
                }
            }

            Map<String, List<Object[]>> rightNaturalMap = new HashMap<String, List<Object[]>>();
            for(Object[] rightRow:rightRows){
                StringBuilder sb = new StringBuilder();
                for(String sameCol:sameCols){
                    sb.append(rightRow[rightColumnIndex.get(sameCol)].toString());
                }

                if(!rightNaturalMap.containsKey(sb.toString())){
                    rightNaturalMap.put(sb.toString(),new ArrayList<Object[]>());
                }

                rightNaturalMap.get(sb.toString()).add(rightRow);
            }

            for(Object[] leftRow: leftRows){
                StringBuilder sb = new StringBuilder();
                for(String sameCol:sameCols){
                    sb.append(leftRow[leftColumnIndex.get(sameCol)].toString());
                }

                if(rightNaturalMap.containsKey(sb.toString())){
                    for(Object[] rightRow:rightNaturalMap.get(sb.toString())){
                        Object[] newRow = new Object[leftRow.length+rightRow.length-sameCols.size()];
                        System.arraycopy(leftRow,0,newRow,0,leftRow.length);

                        int j=leftRow.length;
                        for(int i=0; i<rightRow.length; i++){
                            if(! rightSameColsIndex.contains(i)){
                                newRow[j] = rightRow[i];
                                j++;
                            }
                        }

                        rows.add(newRow);
                    }
                }
            }

            // get the natural rows
//            for(Object[] leftRow: leftRows){
//                for(Object[] rightRow:rightRows){
//                    Boolean natural = true;
//                    for(String sameCol: sameCols){
//                        if (!(leftRow[leftColumnIndex.get(sameCol)].toString().equals(rightRow[rightColumnIndex.get(sameCol)].toString()))){
//                            natural = false;
//                        }
//                    }
//                    if(natural){
//                        Object[] newRow = new Object[leftRow.length+rightRow.length-sameCols.size()];
//                        System.arraycopy(leftRow,0,newRow,0,leftRow.length);
//
//                        int j=leftRow.length;
//                        for(int i=0; i<rightRow.length; i++){
//                            if(! rightSameColsIndex.contains(i)){
//                                newRow[j] = rightRow[i];
//                                j++;
//                            }
//                        }
//
//                        rows.add(newRow);
//                    }
//                }
//            }

            // combine the left/right column index
            Map<String, Integer> newColumnIndex = new TreeMap<String, Integer>();
            for(Map.Entry<String, Integer> lEntry: leftColumnIndex.entrySet()){
                newColumnIndex.put(lEntry.getKey(),lEntry.getValue());
            }
            for (Map.Entry<String, Integer> rEntry: rightColumnIndex.entrySet()){
                if (!newColumnIndex.containsKey(rEntry.getKey())){
                    newColumnIndex.put(rEntry.getKey(),rEntry.getValue()+leftColumnIndex.size()-1);
                }
            }

            result = new XRelation(newColumnIndex, rows);
        }

        return result;
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
