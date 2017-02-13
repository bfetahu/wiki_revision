package revisions;

import utils.Utils;

/**
 * Created by hube on 1/27/2017.
 */
public class TestClass {

    public static void main(String[] args){

        String a = "{{2}}\n" +
                "This is the largest Malaysia Airlines incident, with 280 passengers involved, beating Malaysia Airlines Flight 370, in which 227 passengers were involved.";
        String b = "{{2}}\n" +
                "This is the largest Malaysia Airlines incident, with 280 passengers involved, beating Malaysia Airlines Flight 370, in which 227 passengers were involved.";
        String c = "rtdfdf 23948793847er sdfhgi";

        System.out.println(Utils.getJaccardDistance(a,b));
        System.out.println(Utils.getJaccardDistance(a,c));


        String d = "(mh17/mas17)";
        System.out.println(d.replaceAll("[\\[\\].:,!?;()\"\'{}|=/<>+*]", ""));

    }
}
