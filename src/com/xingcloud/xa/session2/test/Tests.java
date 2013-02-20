package com.xingcloud.xa.session2.test;

import com.xingcloud.xa.session2.exec.PlanExecutor;
import com.xingcloud.xa.session2.parser.Parser;
import com.xingcloud.xa.session2.ra.RelationProvider;
import com.xingcloud.xa.session2.ra.Row;
import com.xingcloud.xa.session2.ra.impl.XRelation;
import net.sf.jsqlparser.JSQLParserException;

import java.util.HashSet;
import java.util.Set;

/**
 * Author: mulisen
 * Date:   2/6/13
 */
public class Tests {

    public static String sql0 = "select count(uid) from user";
	public static String sql1 = "select * from user;";
    public static String sql2 = "select event, uid from event where date='2013-02-01';";
    public static String sql3 = "SELECT COUNT(DISTINCT(uid))\n" +
			                    "FROM (event NATURAL JOIN user)\n" +
			                    "WHERE user.register_time='2013-02-01'\n" +
			                    "AND event.date='2013-02-02' AND event.event='visit';";

    public static String sql4 = "SELECT user.ref0, COUNT(DISTINCT(uid)), SUM(event.value)\n" +
			                    "FROM (event NATURAL JOIN user)\n" +
			                    "WHERE user.register_time='2013-02-01'\n" +
			                    "AND event.date='2013-02-02' AND event.event='visit' " +
								"GROUP BY user.ref0;";


    public static void printResult(RelationProvider relationProvider){
        XRelation.XRow row = null;
        StringBuffer sb = new StringBuffer();
        while ((row = (XRelation.XRow)relationProvider.nextRow())!= null){
            for(Object object:row.rowData){
                sb.append((String) object);
                sb.append("\t");
            }
            sb.append("\n");
        }
        System.out.println(sb.toString());
    }

    public static void  test(){
        Set<String> set = new HashSet<String>();
        set.add("a");
        set.add("b".toString());
        System.out.println(set.contains("b"));
    }

	public static void main(String[] args) throws JSQLParserException {
        //test();
        Tests.printResult(PlanExecutor.executePlan(Parser.getInstance().parse(Tests.sql0)));
        //Tests.printResult(PlanExecutor.executePlan(Parser.getInstance().parse(Tests.sql1)));
		//Tests.printResult(PlanExecutor.executePlan(Parser.getInstance().parse(Tests.sql2)));
		//Tests.printResult(PlanExecutor.executePlan(Parser.getInstance().parse(Tests.sql3)));
		//Tests.printResult(PlanExecutor.executePlan(Parser.getInstance().parse(Tests.sql4)));

    }


}
