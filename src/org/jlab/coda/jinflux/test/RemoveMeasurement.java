package org.jlab.coda.jinflux.test;

import org.jlab.coda.jinflux.JinFlux;
import org.jlab.coda.jinflux.JinFluxException;

/**
 * Class description here....
 * <p>
 *
 * @author gurjyan
 *         Date 4/29/16
 * @version 3.x
 */
public class RemoveMeasurement {
    public static void main(String[] args) {
        String dbName = args[0];
        String measurement = args[1];
        String dbNode = "claraweb.jlab.org";

        try {
            JinFlux jinFlux = new JinFlux(dbNode);
            jinFlux.measureRemove(dbName, measurement);
        } catch (JinFluxException e) {
            e.printStackTrace();
        }
        System.out.println("done");
    }

}
