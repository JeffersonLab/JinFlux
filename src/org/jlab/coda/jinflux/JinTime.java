package org.jlab.coda.jinflux;

/**
 * Class description here....
 * <p>
 *
 * @author gurjyan
 *         Date 4/25/16
 * @version 3.x
 */
public enum JinTime {
    HOURE  ("h"),
    DAY    ("d"),
    WEEK   ("w"),
    INF    ("infinite");


    private String timeMeasure;

    JinTime(String tm) {
        timeMeasure = tm;
    }

    public String weight(){
        return timeMeasure;
    }

}
