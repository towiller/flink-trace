package org.myorg;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class TestDate {

    public static void main(String[] args) throws ParseException {
        //匹配时间日期格式
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");


        String str = "2020-10-10 14:49:37";
        Date t = dateFormat.parse(str);
        System.out.println("gettime:" + t.getTime());
    }
}
