package org.jlab.coda.jinflux.test;

/**
 * Class description here....
 * <p>
 *
 * @author gurjyan
 *         Date 4/27/16
 * @version 3.x
 */
public class JinUtil {

    public static void sleep(int i){
        try {
            Thread.sleep(i);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
