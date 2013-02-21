package com.xingcloud.xa.session2.test;

import com.xingcloud.xa.session2.exec.PlanExecutor;
import com.xingcloud.xa.session2.parser.Parser;
import com.xingcloud.xa.session2.ra.RelationProvider;
import com.xingcloud.xa.session2.ra.Row;
import com.xingcloud.xa.session2.ra.impl.XRelation;
import net.sf.jsqlparser.JSQLParserException;


import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.HashSet;
import java.util.Set;

/**
 * Author: mulisen
 * Date:   2/6/13
 */
public class Tests {

    public static String sql0 = "select * from (event natural join user) group by user.ref0";
	public static String sql1 = "select * from event;";

	public static String sql2 = "select event, uid from event where date='2013-02-01';";

	public static String sql3 = "SELECT COUNT(DISTINCT(uid))\n" +
			                    "FROM (event NATURAL JOIN user)\n" +
			                    "WHERE user.register_time>='20130201000000' AND user.register_time<'20130202000000'\n" +
			                    "AND event.date='2013-02-02' AND event.event='visit';";

    public static String sql3_1 = "SELECT *\n" +
            "FROM (event NATURAL JOIN user)\n" +
            "WHERE user.register_time>='20130201000000' AND user.register_time<'20130202000000'\n" +
            "AND event.date='2013-02-02' AND event.event='visit' group by user.ref0;";

    public static String sql4 = "SELECT user.ref0, COUNT(DISTINCT(uid)), SUM(value)\n" +
			                    "FROM (event NATURAL JOIN user)\n" +
								"WHERE user.register_time>='20130201000000' AND user.register_time<'20130202000000'\n" +
			                    "AND event.date='2013-02-02' AND event.event='visit' " +
								"GROUP BY user.ref0;";


    public static void  test(){
//        Set<String> set = new HashSet<String>();
//        set.add("a");
//        set.add("b".toString());
//        System.out.println(set.contains("b"));
        System.out.println(((Boolean)(2>1)).toString());
    }

	public static void main(String[] args) throws JSQLParserException{
        try{
            //test();
            String file = "data/2/result";
            BufferedWriter bw = new BufferedWriter(new FileWriter(file));
            try{
                //bw.write(sql0+"\n");
                //bw.write(PlanExecutor.executePlan(Parser.getInstance().parse(Tests.sql0)).toString());
                bw.write("\n"+sql1+"\n");
                bw.write(PlanExecutor.executePlan(Parser.getInstance().parse(Tests.sql1)).toString());
                bw.write("\n"+sql2+"\n");
                bw.write(PlanExecutor.executePlan(Parser.getInstance().parse(Tests.sql2)).toString());
                //bw.write("\n"+sql3+"\n");
                //bw.write(PlanExecutor.executePlan(Parser.getInstance().parse(Tests.sql3)).toString());
                //bw.write("\n"+sql3_1+"\n");
                //bw.write(PlanExecutor.executePlan(Parser.getInstance().parse(Tests.sql3_1)).toString());
                bw.write("\n"+sql4+"\n");
                bw.write(PlanExecutor.executePlan(Parser.getInstance().parse(Tests.sql4)).toString());
                bw.flush();
            }catch (Exception ee){

            }finally {
                bw.close();
            }

        }catch (Exception e){

        }
    }


}
