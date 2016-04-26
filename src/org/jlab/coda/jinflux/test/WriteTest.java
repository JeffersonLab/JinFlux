package org.jlab.coda.jinflux.test;

import org.influxdb.dto.Point;
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
public class WriteTest {
    public static void main(String[] args) {

        // connect to the database
        JinFlux jinFlux = null;
        try {
            jinFlux = new JinFlux("claraweb.jlab.org");

        // list existing databases
        System.out.println(jinFlux.dbList());
         if(jinFlux.doesDbExists("afecsRuntime")){

             // remove afecsRuntime database
             jinFlux.dbRemove("afecsRuntime");

         }


        // create a  afecsRuntime database
        jinFlux.dbCreate("afecsRuntime");

        System.out.println(jinFlux.doesDbExists("afecsRuntime"));

        // write some data into afecsRuntime database
        Point.Builder p = jinFlux.open("hallD","runType", "R2");
        jinFlux.write("afecsRuntime",p,"eventRate",111.1);
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        jinFlux.write("afecsRuntime",p,"dataRate",222.2);
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        jinFlux.write("afecsRuntime",p,"eventRate",333.3);
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        jinFlux.write("afecsRuntime",p,"dataRate",444.4);
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }




        // dump the measurement
        jinFlux.dump("afecsRuntime", "hallD");
//        try {
//            Map<Object, Object> m = jinFlux.read("afecsRuntime", "hallD", "eventRate");
//            for(Object o:m.keySet()){
//                System.out.println(o+" "+m.get(o));
//            }
//        } catch (JinFluxException e) {
//            e.printStackTrace();
//        }
        } catch (JinFluxException e) {
            e.printStackTrace();
            System.exit(1);
        }


    }
}
