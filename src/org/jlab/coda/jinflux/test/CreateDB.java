package org.jlab.coda.jinflux.test;

import org.jlab.coda.jinflux.JinFlux;
import org.jlab.coda.jinflux.JinFluxException;
import org.jlab.coda.jinflux.JinTime;

/**
 * Class description here....
 * <p>
 *
 * @author gurjyan
 *         Date 5/2/16
 * @version 3.x
 */
public class CreateDB {
    public static void main(String[] args) {
        String dbName = args[0];
        String dbNode = "claraweb.jlab.org";

        try {
            JinFlux jinFlux = new JinFlux(dbNode);
            jinFlux.dbCreate(dbName, 1, JinTime.HOURE);
        } catch (JinFluxException e) {
            e.printStackTrace();
        }

    }

}
