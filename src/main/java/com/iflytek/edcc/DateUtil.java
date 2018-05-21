package com.iflytek.edcc;

import org.apache.commons.lang3.StringUtils;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * Created with Intellij IDEA.
 * User: ztwu2
 * Date: 2017/11/8
 * Time: 17:35
 * Description
 */

public class DateUtil {

    /**
     * 获取昨天日期
     * @return
     */
    public static String getYesterday(){
        Calendar cal   =   Calendar.getInstance();
        cal.add(Calendar.DATE,   -1);
        String yesterday = new SimpleDateFormat( "yyyy-MM-dd").format(cal.getTime());
        System.out.println(yesterday);
        return yesterday;
    }

    /**
     * 获取当前日期对应的学年范围
     * @param date 日期
     * @param resultType 返回学年边界
     * @return
     */
    public static String getSchoolYear(String date, String resultType) {
        String time = null;
        if(StringUtils.isNotBlank(date) && StringUtils.isNotBlank(resultType)){
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
            Calendar calendar = Calendar.getInstance();
            int year = calendar.get(Calendar.YEAR);

            String splitTime = year + "-" + "08-15";
            String schoolYearStart = null;
            String schoolYearEnd = null;

            Date dateCurrent = null;
            Date dateSplitTime = null;
            try{
                dateCurrent = sdf.parse(date);
                dateSplitTime = sdf.parse(splitTime);
            }catch (Exception e){
                e.printStackTrace();
            }

            if(dateCurrent != null && dateSplitTime != null){
                if(dateCurrent.getTime()>=dateSplitTime.getTime()){
                    schoolYearStart = year + "-" + "08-15";
                    schoolYearEnd = (year+1) + "-" + "08-15";
                }else {
                    schoolYearStart = (year-1) + "-" + "08-15";
                    schoolYearEnd = year + "-" + "08-15";
                }

                if("1".equals(resultType)){
                    time = schoolYearStart;
                }else if("2".equals(resultType)) {
                    time = schoolYearEnd;
                }
            }
			System.out.println(time);
        }
        return time;
    }

    public static void main(String[] args){
        getYesterday();
//        getSchoolYear("2017-11-24","2");
    }

}
