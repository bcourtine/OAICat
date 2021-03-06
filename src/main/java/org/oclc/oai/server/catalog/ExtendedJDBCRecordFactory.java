/**
 * Copyright 2006 OCLC Online Computer Library Center Licensed under the Apache
 * License, Version 2.0 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable law or
 * agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.oclc.oai.server.catalog;

import java.sql.Timestamp;
import java.util.*;

/** ExtendedJDBCRecordFactory converts JDBC "items" to "record" Strings. */
public class ExtendedJDBCRecordFactory extends RecordFactory {
    private String repositoryIdentifier = null;
    protected String identifierLabel = null;
    protected String datestampLabel = null;

    /**
     * Construct an ExtendedJDBCRecordFactory capable of producing the Crosswalk(s)
     * specified in the properties file.
     *
     * @param properties Contains information to configure the factory:
     * specifically, the names of the crosswalk(s) supported
     * @throws IllegalArgumentException Something is wrong with the argument.
     */
    public ExtendedJDBCRecordFactory(Properties properties)
            throws IllegalArgumentException {
        super(properties);
        repositoryIdentifier = properties.getProperty("ExtendedJDBCRecordFactory.repositoryIdentifier");
        if (repositoryIdentifier == null) {
            throw new IllegalArgumentException("ExtendedJDBCRecordFactory.repositoryIdentifier is missing from the properties file");
        }
        identifierLabel = properties.getProperty("ExtendedJDBCRecordFactory.identifierLabel");
        if (identifierLabel == null) {
            throw new IllegalArgumentException("ExtendedJDBCRecordFactory.identifierLabel is missing from the properties file");
        }
        datestampLabel = properties.getProperty("ExtendedJDBCRecordFactory.datestampLabel");
        if (datestampLabel == null) {
            throw new IllegalArgumentException("ExtendedJDBCRecordFactory.datestampLabel is missing from the properties file");
        }
    }

    /**
     * Utility method to parse the 'local identifier' from the OAI identifier
     *
     * @param oaiIdentifier OAI identifier (e.g. oai:oaicat.oclc.org:ID/12345)
     * @return local identifier (e.g. ID/12345).
     */
    public String fromOAIIdentifier(String oaiIdentifier) {
        StringTokenizer tokenizer = new StringTokenizer(oaiIdentifier, ":");
        try {
            tokenizer.nextToken();
            tokenizer.nextToken();
            return tokenizer.nextToken();
        } catch (java.util.NoSuchElementException e) {
            return null;
        }
    }

    /**
     * Extract the local identifier from the native item
     *
     * @param nativeItem native Item object
     * @return local identifier
     */
    public String getLocalIdentifier(Object nativeItem) {
        Map<String, Object> table = (Map<String, Object>) ((Map<String, Object>) nativeItem).get("coreResult");
        return table.get(identifierLabel).toString();
    }

    /**
     * Construct an OAI identifier from the native item
     *
     * @param nativeItem native Item object
     * @return OAI identifier
     */
    public String getOAIIdentifier(Object nativeItem) throws IllegalArgumentException {
        StringBuilder sb = new StringBuilder();
        sb.append("oai:");
        sb.append(repositoryIdentifier);
        sb.append(":");
        sb.append(getLocalIdentifier(nativeItem));
        return sb.toString();
    }

    /**
     * get the datestamp from the item
     *
     * @param nativeItem a native item presumably containing a datestamp somewhere
     * @return a String containing the datestamp for the item
     * @throws IllegalArgumentException Something is wrong with the argument.
     */
    public String getDatestamp(Object nativeItem) {
        Map<String, Object> table = (HashMap) ((HashMap) nativeItem).get("coreResult");
        return ((Timestamp) table.get(datestampLabel)).toString().substring(0, 10);
    }

    /**
     * get the setspec from the item
     *
     * @param nativeItem a native item presumably containing a setspec somewhere
     * @return a String containing the setspec for the item. Null if setSpecs aren't
     *         derived from the nativeItem.
     * @throws IllegalArgumentException Something is wrong with the argument.
     */
    public Iterator getSetSpecs(Object nativeItem) throws IllegalArgumentException {
        return null;
    }

    /**
     * Get the about elements from the item
     *
     * @param nativeItem a native item presumably containing about information somewhere
     * @return a Iterator of Strings containing &lt;about&gt;s for the item. Null if
     *         abouts aren't derived from the nativeItem
     * @throws IllegalArgumentException Something is wrong with the argument.
     */
    public Iterator getAbouts(Object nativeItem) throws IllegalArgumentException {
        return null;
    }

    /**
     * Is the record deleted?
     *
     * @param nativeItem a native item presumably containing a possible delete indicator
     * @return true if record is deleted, false if not
     * @throws IllegalArgumentException Something is wrong with the argument.
     */
    public boolean isDeleted(Object nativeItem) throws IllegalArgumentException {
        return false;
    }

    /**
     * Allows classes that implement RecordFactory to override the default create() method.
     * This is useful, for example, if the entire &lt;record&gt; is already packaged as the native
     * record. Return null if you want the default handler to create it by calling the methods
     * above individually.
     *
     * @param nativeItem the native record
     * @return a String containing the OAI &lt;record&gt; or null if the default method should be used.
     */
    public String quickCreate(Object nativeItem, String schemaLocation, String metadataPrefix) {
        // Don't perform quick creates
        return null;
    }
}
