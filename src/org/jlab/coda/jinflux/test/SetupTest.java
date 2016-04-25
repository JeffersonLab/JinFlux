package org.jlab.coda.jinflux.test;

import org.jlab.coda.jinflux.JinFlux;
import org.jlab.coda.jinflux.JinTime;

/**
 * Class description here....
 * <p>
 *
 * @author gurjyan
 *         Date 4/25/16
 * @version 3.x
 */
public class SetupTest {
    public static void main(String[] args) {

        String dbName = args[0];

        // connect to the database
        JinFlux jinFlux = new JinFlux("claraweb.jlab.org");
//        jinFlux.dbRemove(dbName);
        jinFlux.dbList().forEach(System.out::println);

        //reset the database. This will create a database if it does not exists
        jinFlux.resetDb(dbName);

        jinFlux.dbList().forEach(System.out::println);

        // set the default retention policy. here we et it top be 1 hour
        jinFlux.rpCreate(dbName, 1, JinTime.HOURE);

//        jinFlux.rpRemove(dbName,"DAY_1");


        // print the default retention policy
        System.out.println(jinFlux.rpShow(dbName));

    }
}
