package org.jlab.coda.jinflux.test;

import org.jlab.coda.jinflux.JinFlux;
import org.jlab.coda.jinflux.JinFluxException;

import java.util.Map;

/**
 * Class description here....
 * <p>
 *
 * @author gurjyan
 *         Date 4/19/16
 * @version 3.x
 */
public class JinTest {
    public static void main(String[] args) {

        // connect to the database
        JinFlux jinFlux = new JinFlux("claraweb.jlab.org");

        // list existing databases
        System.out.println(jinFlux.dbList());

        // create a org.jlab.coda.jinflux.test database
        jinFlux.dbCreate("myTest");

        // check to see if the org.jlab.coda.jinflux.test database is created
        System.out.println("database myTest exists = " + jinFlux.doesDbExists("myTest"));

        // remove myTest database
        jinFlux.dbRemove("myTest");

        // check to see if the org.jlab.coda.jinflux.test database is removed
        System.out.println("database myTest exists = " + jinFlux.doesDbExists("myTest"));


        // write some data into afecsRuntime database
//        jinFlux.writeSingle("afecsRuntime", "expid", "EventRate", 6000);
//        jinFlux.writeSingle("afecsRuntime", "expid", "DataRate", 6000);

        // dump the measurement
//        jinFlux.dump("afecsRuntime", "expid", "*");
        try {
            Map<Object, Object> m = jinFlux.read("afecsRuntime", "expid", "EventRate");
            for(Object o:m.keySet()){
                System.out.println(o+" "+m.get(o));
            }
        } catch (JinFluxException e) {
            e.printStackTrace();
        }


    }
}
