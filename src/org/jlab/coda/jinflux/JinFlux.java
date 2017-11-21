/*
 *   Copyright (c) 2016.  Jefferson Lab (JLab). All rights reserved. Permission
 *   to use, copy, modify, and distribute  this software and its documentation for
 *   educational, research, and not-for-profit purposes, without fee and without a
 *   signed licensing agreement.
 *
 *   IN NO EVENT SHALL JLAB BE LIABLE TO ANY PARTY FOR DIRECT, INDIRECT, SPECIAL
 *   INCIDENTAL, OR CONSEQUENTIAL DAMAGES, INCLUDING LOST PROFITS, ARISING
 *   OUT OF THE USE OF THIS SOFTWARE AND ITS DOCUMENTATION, EVEN IF JLAB HAS
 *   BEEN ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 *   JLAB SPECIFICALLY DISCLAIMS ANY WARRANTIES, INCLUDING, BUT NOT LIMITED TO,
 *   THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
 *   PURPOSE. THE CLARA SOFTWARE AND ACCOMPANYING DOCUMENTATION, IF ANY,
 *   PROVIDED HEREUNDER IS PROVIDED "AS IS". JLAB HAS NO OBLIGATION TO PROVIDE
 *   MAINTENANCE, SUPPORT, UPDATES, ENHANCEMENTS, OR MODIFICATIONS.
 *
 *   This software was developed under the United States Government license.
 *   For more information contact author at gurjyan@jlab.org
 *   Department of Experimental Nuclear Physics, Jefferson Lab.
 */

package org.jlab.coda.jinflux;

import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Point;
import org.influxdb.dto.Pong;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Time series database Java API.
 * Uses InfluxDB java binding as a base.
 * <p>
 *
 * @author gurjyan
 *         Date 4/19/16
 * @version 1.x
 */
public class JinFlux {
    private InfluxDB influxDB;

    /**
     * Constructor
     * Opens connection to the InfluxDB server using authentication.
     * Enables batch mode where we flush every 1000 points at least
     * every 100 milli seconds (which one comes first)
     * <p>
     *
     * @param influxDbHost host of the InfluxDB server
     * @param port port number of the InfluxDB server
     * @param user database username
     * @param password password
     * @throws JinFluxException exception
     */
    public JinFlux(String influxDbHost, int port, String user, String password) throws JinFluxException {
        try {
            this.influxDB = InfluxDBFactory.connect("http://" + influxDbHost + ":"+port, user, password);

            // Flush every 1000 points, at least every 100ms (which one comes first)
            this.influxDB.enableBatch(1000, 100, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            throw new JinFluxException(e.getMessage());
        }
    }

    /**
     * Constructor
     * Opens connection to the InfluxDB server using authentication.
     * Uses default server port = 8086
     * <p>
     *
     * @param influxDbHost host of the InfluxDB server
     * @param user database username
     * @param password password
     * @throws JinFluxException exception
     */
    public JinFlux(String influxDbHost, String user, String password) throws JinFluxException {
        this(influxDbHost,8086,user,password);
     }

    /**
     * Constructor
     * Opens connection to the InfluxDB server using default authentication and port number.
     * <p>
     *
     * @param influxDbHost
     * @throws JinFluxException
     */
    public JinFlux(String influxDbHost) throws JinFluxException {
        this(influxDbHost, 8086, "root", "root");
    }

    /**
     * Shows retention policy, configured for a database
     * <p>
     *
     * @param dbName database name
     * @return String representing the retention policy
     * @throws JinFluxException
     */
    public String showRP(String dbName) throws JinFluxException {
        Query query = new Query("SHOW RETENTION POLICIES ON " + dbName, dbName);
        try {
            QueryResult r = influxDB.query(query);

            boolean isRpDefault = false;

            if (r.getResults() != null) {
                for (QueryResult.Result qr : r.getResults()) {
                    for (QueryResult.Series sr : qr.getSeries()) {
                        for (List<Object> l : sr.getValues()) {
                            boolean first = true;
                            StringBuilder sb = new StringBuilder();
                            for (Object ll : l) {
                                sb.append(ll).append(" ");
                                if (first) {
                                    isRpDefault = false;
                                    first = false;
                                } else {
                                    if (ll instanceof Boolean) {
                                        Boolean tmp = (Boolean) ll;
                                        if (tmp) isRpDefault = true;
                                    }
                                }
                            }
                            if (isRpDefault) {
                                return sb.toString();
                            }
                        }
                    }
                }
            }
        } catch (Exception e) {
            throw new JinFluxException(e.getMessage());
        }
        return null;
    }

    /**
     * Ping the database server to see if it is up and running
     * <p>
     *
     * @param timeout in seconds
     * @return true if database is up and running
     * @throws Exception
     */
    public boolean ping(int timeout) throws Exception {
        int tries = 0;
        timeout = timeout * 10;
        boolean influxStarted = false;
        do {
            Pong response = this.influxDB.ping();

            if (!response.getVersion().equalsIgnoreCase("unknown")) {
                influxStarted = true;
            }
            Thread.sleep(100L);
            tries++;
        } while (!influxStarted && (tries < timeout));

        return (tries < timeout);
    }

    /**
     * Creates a database with specific data retention policy
     * <p>
     *
     * @param dbName database name
     * @param retentionTime retention time
     * @param tm retention time unite {@link JinTime}
     * @param rpName retention policy name
     * @return true if succeeded to create the database
     * @throws JinFluxException
     */
    public boolean createDB(String dbName, int retentionTime, JinTime tm, String rpName)
            throws JinFluxException {
        if(existsDB(dbName)){
            removeDB(dbName);
        }
        Query query = new Query("CREATE DATABASE " + dbName +
                " WITH DURATION " + retentionTime+tm.weight()+" REPLICATION 1 NAME "+rpName, dbName);
        try {
            influxDB.query(query);
        } catch (Exception e) {
            throw new JinFluxException(e.getMessage());
        }
        return existsDB(dbName);
    }

    /**
     * Creates a database with specific data retention.
     * The default name = rpafecs is assigned to the policy
     * <p>
     *
     * @param dbName database name
     * @param retentionTime retention time
     * @param tm retention time unite {@link JinTime}
     * @return true if succeeded to create the database
     * @throws JinFluxException
     */
    public boolean createDB(String dbName, int retentionTime, JinTime tm)
            throws JinFluxException {
        return createDB(dbName, retentionTime, tm, "rpafecs");
     }

    /**
     * Creating a database with the default retention policy:
     * retention of the data = forever and the policy name = default
     * <p>
     *
     * @param dbName the name of the database
     * @return true if succeeded to create the database
     * @throws JinFluxException
     */
    public boolean createDB(String dbName)
            throws JinFluxException {
        influxDB.createDatabase(dbName);
        return existsDB(dbName);
    }

    /**
     * Recreating a database with the default retention policy:
     * retention of the data = forever and the policy name = default
     * The existing database will be removed first.
     * <p>
     *
     * @param dbName the name of the databse
     * @return true if succeeded to create the database
     * @throws JinFluxException
     */
    public boolean recreateDB(String dbName) throws JinFluxException {
        if (existsDB(dbName)) removeDB(dbName);
        return createDB(dbName);

    }

    /**
     * Removes the database
     * <p>
     *
     * @param dbName the name of the database
     * @throws JinFluxException
     */
    public void removeDB(String dbName) throws JinFluxException {
        try {
            influxDB.deleteDatabase(dbName);
        } catch (Exception e) {
            throw new JinFluxException(e.getMessage());
        }
    }

    /**
     * Lists of all databases
     * <p>
     *
     * @return List of database names
     * @throws JinFluxException
     */
    public List<String> listDB() throws JinFluxException {
        try {
            return influxDB.describeDatabases();
        } catch (Exception e) {
            throw new JinFluxException(e.getMessage());
        }
    }

    /**
     * Checks if the database exists.
     * <p>
     *
     * @param dbName the name of the database
     * @return true if the database exists
     * @throws JinFluxException
     */
    public boolean existsDB(String dbName) throws JinFluxException {
        boolean exists = false;
        List<String> result = listDB();
        if (result != null && result.size() > 0) {
            for (String database : result) {
                if (database.equals(dbName)) {
                    exists = true;
                    break;
                }
            }
        }
        return exists;
    }

    /**
     * Creates a table builder.
     * Note: this will not create an empty table in the database.
     * <p>
     *
     * @param tbName the name of the table
     * @return pointer to the table
     */
    public Point.Builder openTB(String tbName) {

        return Point.measurement(tbName);
    }

    /**
     * Creates a table builder, with an additional tag (in addition to time)
     * Note: this will not create an empty table in the database.
     * <p>
     *
     * @param tbName the name of the table
     * @param tagName the name of the tag
     * @param value the value of the tag
     * @return pointer to the table
     */
    public Point.Builder openTB(String tbName,
                                String tagName,
                                String value) {

        return Point.measurement(tbName).tag(tagName, value);
    }

    /**
     * Creates a table builder, with additional tags (in addition to time)
     * Note: this will not create an empty table in the database.
     * <p>
     *
     * @param tbName the name of the table
     * @param tags the map of table tag value pairs
     * @return pointer to the table
     */
    public Point.Builder openTB(String tbName,
                                Map<String, String> tags) {

        return Point.measurement(tbName).tag(tags);
    }

    /**
     * Remove the existing table from the database.
     * <p>
     *
     * @param dbName the of the database
     * @param tbName the name of the tbName
     * @throws JinFluxException
     */
    public void removeTB(String dbName, String tbName) throws JinFluxException {
        Query query = new Query("DROP MEASUREMENT " + tbName, dbName);
        try {
            influxDB.query(query);
        } catch (Exception e) {
            throw new JinFluxException(e.getMessage());
        }
    }

    /**
     * Dumps the content of the database recorded table on the console.
     *
     * @param dbName the name of the database
     * @param tbName the name of the table
     */
    public void dumpTB(String dbName, String tbName) throws JinFluxException {
        Query query = new Query("SELECT * FROM " + tbName, dbName);

        try {
            QueryResult r = influxDB.query(query);

            System.out.println(r);
            printQueryResult(r);
        } catch (Exception e) {
            throw new JinFluxException(e.getMessage());
        }
    }

    /**
     * Adds a data point to the table. Accepts types, such as any Number,
     * String, Boolean, Long, Double, Float, as well as list of Numbers
     * <p>
     *
     * @param point reference to the table
     * @param dpName the name of the data point
     * @param value the value of the data point
     * @return reference to the table builder
     */
    public Point.Builder addDP(Point.Builder point,
                               String dpName,
                               Object value) {

        if (value instanceof String) {
            point.addField(dpName, (String) value);

        } else if (value instanceof Boolean) {
            point.addField(dpName, (Boolean) value);

        } else if (value instanceof Long) {
            point.addField(dpName, (Long) value);

        } else if (value instanceof Double) {
            point.addField(dpName, (Double) value);

        } else if (value instanceof Float) {
            point.addField(dpName, (Float) value);

        } else if (value instanceof Number) {
            point.addField(dpName, (Number) value);

        }else if (value instanceof List) {
            List l = (List)value;
            for(int i=0; i<l.size();i++) {
                if(l.get(i) instanceof Number) {
                    point.addField(dpName + "_" + i, (Number)l.get(i));
                } else break;
            }
        }
        return point;
    }

    /**
     * Adds a group of data points to the table
     * <p>
     *
     * @param point reference to the table builder
     * @param dpNames map of data point names and values
     * @return reference to the table builder
     */
    public Point.Builder addDP(Point.Builder point,
                               Map<String, Object> dpNames) {

        for (String tag : dpNames.keySet()) {
            addDP(point, tag, dpNames.get(tag));
        }
        return point;
    }

    /**
     * Writes the table into the database. The time is defined and
     * recorded for all the data points od the table. If the
     * table does not exists it will be created.
     * <p>
     *
     * @param dbName the name of the database
     * @param spot reference to the table builder
     * @throws JinFluxException
     */
    public void write(String dbName,
                      Point.Builder spot) throws JinFluxException {

        spot.time(System.currentTimeMillis(), TimeUnit.MILLISECONDS);
        // write the point to the database
            influxDB.write(dbName, "rpafecs", spot.build());
    }

    /**
     * Selects the data points, having the specified tag from a table of the database.
     * <p>
     *
     * @param dbName the name of the database
     * @param tbName the name of the table
     * @param tag the name of the tag
     * @return map of data points, such as data point name and values
     * @throws JinFluxException
     */
    public Map<Object, Object> read(String dbName, String tbName, String tag) throws JinFluxException {
        if (tag.equals("*")) throw new JinFluxException("wildcards are not supported");
        Map<Object, Object> rm = new LinkedHashMap<>();
        Object o1 = null;
        Object o2 = null;
        Query query = new Query("SELECT " + tag + " FROM " + tbName, dbName);
        try {
            QueryResult r = influxDB.query(query);
            if (r != null) {
                for (QueryResult.Result qr : r.getResults()) {
                    for (QueryResult.Series sr : qr.getSeries()) {
                        for (List<Object> l : sr.getValues()) {
                            boolean first = true;
                            for (Object ll : l) {
                                if (first) {
                                    o1 = ll;
                                    first = false;
                                } else {
                                    o2 = ll;
                                    first = true;
                                }
                                if (first) {
                                    rm.put(o1, o2);
                                }
                            }
                        }
                    }
                }
            }
        } catch (Exception e) {
            throw new JinFluxException(e.getMessage());
        }
        return rm;
    }

    /**
     * Returns the list of tags of a table
     * <p>
     *
     * @param dbName the name of the database
     * @param tbName the tame of the table
     * @return List of tags names
     * @throws JinFluxException
     */
    public List<String> readTags(String dbName, String tbName) throws JinFluxException {
        List<String> rl = new ArrayList<>();
        Query query = new Query("SELECT *  FROM " + tbName, dbName);
        try {
            QueryResult r = influxDB.query(query);

            for (QueryResult.Result qr : r.getResults()) {
                for (QueryResult.Series sr : qr.getSeries()) {
                    List<String> l = sr.getColumns();
                    rl.addAll(l);
                }
            }
        } catch (Exception e) {
            throw new JinFluxException(e.getMessage());
        }
        return rl;
    }

    /**
     * Private method to print the query result on the console
     * @param r query result
     */
    private void printQueryResult(QueryResult r) {
        if (r.getResults() != null) {
            for (QueryResult.Result qr : r.getResults()) {
                for (QueryResult.Series sr : qr.getSeries()) {
                    System.out.println("===================================================");
                    System.out.println("              " + sr.getName());
                    System.out.println("---------------------------------------------------");

                    for (String column : sr.getColumns()) {
                        if (column.equals("time"))
                            System.out.print(column + "                        ");
                        else System.out.print(column + "\t");
                    }
                    System.out.println();
                    System.out.println("---------------------------------------------------");
                    for (List<Object> l : sr.getValues()) {
                        boolean first = true;
                        for (Object ll : l) {
                            if (first) {
                                System.out.print(ll + "\t");
                                first = false;
                            } else {
                                System.out.print(ll + "\t\t");
                            }
                        }
                        System.out.println();
                    }
                    System.out.println("===================================================");
                }
            }
        }
    }
}
