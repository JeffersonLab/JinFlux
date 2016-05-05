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
public class RemoveDB {
    public static void main(String[] args) {
        String dbName = args[0];
        String dbNode = "claraweb.jlab.org";

        try {
            JinFlux jinFlux = new JinFlux(dbNode);
            jinFlux.removeDB(dbName);
        } catch (JinFluxException e) {
            e.printStackTrace();
        }

    }

}
