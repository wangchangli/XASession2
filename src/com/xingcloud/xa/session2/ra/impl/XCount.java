package com.xingcloud.xa.session2.ra.impl;

import com.xingcloud.xa.session2.ra.*;

/**
 * Author: mulisen
 * Date:   2/7/13
 */
public class XCount extends AbstractAggregation implements Count {
	RelationProvider relation;
	public Aggregation setInput(RelationProvider relation) {
        resetInput();
		init();
		this.relation = relation;
		addInput(relation);
		return this;
	}

    public void updateRelation(RelationProvider relation){
        if(this.relation instanceof XDistinct){
            ((XDistinct)this.relation).updateRelation(relation);
            ((XDistinct)this.relation).result = null;
        }
    }

	public Object aggregate() {
		//return null;  //TODO method implementation
        Long l = 0L;
        RowIterator iterator = relation.iterator();
        while (iterator.hasNext()){
            iterator.nextRow();
            l++;
        }
        return String.valueOf(l);
	}

	public void init() {
		//TODO method implementation
	}
}
