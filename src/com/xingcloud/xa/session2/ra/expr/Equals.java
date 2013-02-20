package com.xingcloud.xa.session2.ra.expr;

import com.xingcloud.xa.session2.ra.Row;
import sun.font.TrueTypeFont;

/**
 * Author: mulisen
 * Date:   2/6/13
 */
public class Equals extends BinaryExpression  {

	public Equals(Expression left, Expression right) {
		super(left, right);

	}

	public Object evaluate(Row input) {
        try{
		    return left.evaluate(input).equals(right.evaluate(input));
        }catch (Exception e){
            return true;
        }
	}
}
