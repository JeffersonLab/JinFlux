package org.jlab.coda.jinflux.test;

import org.jlab.coda.jinflux.JinFlux;
import org.jlab.coda.jinflux.JinFluxException;

/**
 * Class description here....
 * <p>
 *
 * @author gurjyan
 *         Date 4/27/16
 * @version 3.x
 */
public class DumpMeasurement {
    public static void main(String[] args) {
        String dbName = args[0];
        String dbNode = "claraweb.jlab.org";
        String expid = args[1];

        // connect to the database
        try {

            JinFlux jinFlux = new JinFlux(dbNode);

            // dump the measurement
            jinFlux.dumpTB(dbName, expid);

        } catch (JinFluxException e) {
            e.printStackTrace();
            System.exit(1);
        }

    }
}
