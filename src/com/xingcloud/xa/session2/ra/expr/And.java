package com.xingcloud.xa.session2.ra.expr;

import com.xingcloud.xa.session2.ra.Row;

/**
 * Author: mulisen
 * Date:   2/7/13
 */
public class And extends BinaryExpression  {

	public And(Expression left, Expression right) {
		super(left, right);
	}

	public Object evaluate(Row input) {
		//return Boolean.parseBoolean(((Boolean)left.evaluate(input)).toString()) && Boolean.parseBoolean(((Boolean)right.evaluate(input)).toString());
        left.evaluate(input);
        if (right == null){
            System.out.print("xxxxxxddddddd");
        }
        right.evaluate(input);
        return (Boolean)left.evaluate(input) && (Boolean)right.evaluate(input);
	}
}
