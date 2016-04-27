package org.jlab.coda.jinflux.test;

import org.jlab.coda.jinflux.JinFlux;
import org.jlab.coda.jinflux.JinFluxException;

/**
 * Class description here....
 * <p>
 *
 * @author gurjyan
 *         Date 4/25/16
 * @version 3.x
 */
public class ShowRetentionPolicy {
    public static void main(String[] args) {

        String dbName = args[0];

        // connect to the database
        JinFlux jinFlux;
        try {
            jinFlux = new JinFlux("claraweb.jlab.org");

        // print the default retention policy
        System.out.println(jinFlux.rpShow(dbName));

        } catch (JinFluxException e) {
            e.printStackTrace();
            System.exit(1);
        }

    }
}
