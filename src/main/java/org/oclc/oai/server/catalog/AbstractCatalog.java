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

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.*;

import javax.servlet.ServletContext;

import org.oclc.oai.server.crosswalk.Crosswalks;
import org.oclc.oai.server.verb.BadArgumentException;
import org.oclc.oai.server.verb.BadResumptionTokenException;
import org.oclc.oai.server.verb.CannotDisseminateFormatException;
import org.oclc.oai.server.verb.IdDoesNotExistException;
import org.oclc.oai.server.verb.NoItemsMatchException;
import org.oclc.oai.server.verb.NoMetadataFormatsException;
import org.oclc.oai.server.verb.NoSetHierarchyException;
import org.oclc.oai.server.verb.OAIInternalServerError;
import org.oclc.oai.server.verb.ServerVerb;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * AbstractCatalog is the generic interface between OAICat and any arbitrary
 * database. Implement this interface to have OAICat work with your database.
 *
 * @author Jeffrey A. Young, OCLC Online Computer Library Center
 */
public abstract class AbstractCatalog {

    /** Class logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractCatalog.class);


    /** The RecordFactory that understands how to convert this database's native "item" to the various metadataFormats to be supported. */
    private RecordFactory recordFactory;

    /** is this repository harvestable? */
    private boolean harvestable = true;

    /** optional property to limit the life of resumptionTokens (<0 indicates no limit) */
    private int millisecondsToLive = -1;

    /** Index into VALID_GRANULARITIES and FROM_GRANULARITIES */
    private int supportedGranularityOffset = -1;

    /** All possible valid granularities */
    private static final String[] VALID_GRANULARITIES = {
            "YYYY-MM-DD",
            "YYYY-MM-DDThh:mm:ssZ"
    };

    /** minimum valid 'from' granularities */
    private static final String[] FROM_GRANULARITIES = {
            "0000-01-01",
            "0000-01-01T00:00:00Z"
    };

    /**
     * return a handle to the RecordFactory
     *
     * @return guess
     */
    public RecordFactory getRecordFactory() {
        return recordFactory;
    }

    public void setHarvestable(boolean harvestable) {
        this.harvestable = harvestable;
    }

    /**
     * Is this repository harvestable?
     *
     * @return true if harvestable, false otherwise.
     */
    public boolean isHarvestable() {
        return harvestable;
    }

    /**
     * get the optional millisecondsToLive property (<0 indicates no limit)
     *
     * @return Resumption token time to live
     */
    public int getMillisecondsToLive() {
        return millisecondsToLive;
    }

    public void setRecordFactory(RecordFactory recordFactory) {
        this.recordFactory = recordFactory;
    }

    public void setSupportedGranularityOffset(int i) {
        supportedGranularityOffset = i;
    }

    /**
     * Convert the requested 'from' parameter to the finest granularity supported by this repository.
     *
     * @param from Request 'from' parameter.
     * @return From parameter converted to the finest granularity.
     * @throws BadArgumentException one or more of the arguments are bad.
     */
    public String toFinestFrom(String from) throws BadArgumentException {
        LOGGER.debug("AbstractCatalog.toFinestFrom: from=" + from);
        LOGGER.debug("AbstractCatalog.toFinestFrom: target=" + VALID_GRANULARITIES[supportedGranularityOffset]);

        if (from.length() > VALID_GRANULARITIES[supportedGranularityOffset].length()) {
            throw new BadArgumentException();
        }
        if (from.length() != VALID_GRANULARITIES[supportedGranularityOffset].length()) {
            StringBuilder sb = new StringBuilder(from);
            if (sb.charAt(sb.length() - 1) == 'Z') {
                sb.setLength(sb.length() - 1);
            }

            sb.append(FROM_GRANULARITIES[supportedGranularityOffset].substring(sb.length()));
            from = sb.toString();
        }

        if (!isValidGranularity(from)) {
            throw new BadArgumentException();
        }

        return from;
    }

    /**
     * Convert the requested 'until' paramter to the finest granularity supported by this repository
     *
     * @param until Request 'until' parameter.
     * @return Until parameter converted to the finest granularity.
     * @throws BadArgumentException one or more of the arguments are bad.
     */
    public String toFinestUntil(String until) throws BadArgumentException {
        if (until.length() == VALID_GRANULARITIES[supportedGranularityOffset].length()) {
            if (!isValidGranularity(until)) {
                throw new BadArgumentException();
            }
            return until;
        }
        if (until.length() > VALID_GRANULARITIES[supportedGranularityOffset].length()) {
            throw new BadArgumentException();
        }

        StringBuilder sb = new StringBuilder(until);
        if (sb.charAt(sb.length() - 1) == 'Z') {
            sb.setLength(sb.length() - 1);
        }

        if (sb.length() < VALID_GRANULARITIES[0].length()) {
            while (sb.length() < 4) {
                sb.append("9");
            }
            switch (sb.length()) {
                case 4: // YYYY
                    sb.append("-");
                case 5: // YYYY-
                    sb.append("12");
                case 7: // YYYY-MM
                    sb.append("-");
                case 8: // YYYY-MM-
                    sb.append("31");
                    break;

                case 6: // YYYY-M
                case 9: // YYYY-MM-D
                    throw new BadArgumentException();
            }
        }

        until = sb.toString();
        if (until.length() == VALID_GRANULARITIES[supportedGranularityOffset].length()) {
            if (!isValidGranularity(until)) {
                throw new BadArgumentException();
            }
            return until;
        }

        if (sb.length() < VALID_GRANULARITIES[1].length()) {
            switch (sb.length()) {
                case 10: // YYYY-MM-DD
                    sb.append("T");
                case 11: // YYYY-MM-DDT
                    sb.append("23");
                case 13: // YYYY-MM-DDThh
                    sb.append(":");
                case 14: // YYYY-MM-DDThh:
                    sb.append("59");
                case 16: // YYYY-MM-DDThh:mm
                    sb.append(":");
                case 17: // YYYY-MM-DDThh:mm:
                    sb.append("59");
                case 19: // YYYY-MM-DDThh:mm:ss
                    sb.append("Z");
                    break;

                case 18: // YYYY-MM-DDThh:mm:s
                    throw new BadArgumentException();
            }
        }

        until = sb.toString();
        if (!isValidGranularity(until)) {
            throw new BadArgumentException();
        }
        return until;
    }

    /**
     * Does the specified date conform to the supported granularity of this repository?
     *
     * @param date a UTC date
     * @return true if date conforms to the supported granularity of this repository, false otherwise.
     */
    private boolean isValidGranularity(String date) {
        if (date.length() > VALID_GRANULARITIES[supportedGranularityOffset].length()) {
            return false;
        }

        if (date.length() < VALID_GRANULARITIES[0].length()
                || !Character.isDigit(date.charAt(0)) // YYYY
                || !Character.isDigit(date.charAt(1))
                || !Character.isDigit(date.charAt(2))
                || !Character.isDigit(date.charAt(3))
                || date.charAt(4) != '-'
                || !Character.isDigit(date.charAt(5)) // MM
                || !Character.isDigit(date.charAt(6))
                || date.charAt(7) != '-'
                || !Character.isDigit(date.charAt(8)) // DD
                || !Character.isDigit(date.charAt(9))) {
            return false;
        }

        if (date.length() > VALID_GRANULARITIES[0].length()) {
            if (date.charAt(10) != 'T'
                    || date.charAt(date.length() - 1) != 'Z'
                    || !Character.isDigit(date.charAt(11)) // hh
                    || !Character.isDigit(date.charAt(12))
                    || date.charAt(13) != ':'
                    || !Character.isDigit(date.charAt(14)) // mm
                    || !Character.isDigit(date.charAt(15))
                    || date.charAt(16) != ':'
                    || !Character.isDigit(date.charAt(17)) // ss
                    || !Character.isDigit(date.charAt(18))) {
                return false;
            }
        }

        return true;
    }

    /**
     * Retrieve the Crosswalks property
     *
     * @return the Crosswalks object containing a detailed list of oai formats supported by this application.
     */
    public Crosswalks getCrosswalks() {
        return recordFactory.getCrosswalks();
    }

    /**
     * Retrieve the list of supported Sets. This should probably be initialized by the constructor from the properties object that is passed to it.
     *
     * @return a Map object containing <setSpec> values as the Map keys and <setName> values for the corresponding the Map values.
     * @throws NoSetHierarchyException No sets are defined for this repository
     * @throws OAIInternalServerError An error occurred
     */
    public abstract Map<String, Object> listSets() throws NoSetHierarchyException, OAIInternalServerError;

    /**
     * Retrieve the next cluster of supported sets.
     *
     * @param resumptionToken OAI resumption token.
     * @return a Map object containing <setSpec> values as the Map keys and
     *         <setName> values for the corresponding the Map values.
     * @throws BadResumptionTokenException The resumptionToken is bad.
     * @throws OAIInternalServerError An error occurred
     */
    public abstract Map<String, Object> listSets(String resumptionToken) throws BadResumptionTokenException, OAIInternalServerError;

    /**
     * Factory method for creating an AbstractCatalog instance. The properties object must contain the following entries:
     * <ul>
     * <li><b>AbstractCatalog.className</b> property which points to a class that implements the AbstractCatalog interface.
     * Note that this class must have a constructor that accepts a properties object as a parameter.</li>
     * <li><b>Crosswalks.&lt;supported formats&gt;</b> properties which satisfy the constructor for the Crosswalks class</li>
     * </ul>
     *
     * @param properties Properties object containing entries necessary to initialize the class to be created.
     * @param context Servlet context, containing entries to initialize the class to be created.
     * @return on object instantiating the AbstractCatalog interface.
     * @throws Throwable some sort of problem occurred.
     */
    public static AbstractCatalog factory(Properties properties, ServletContext context) throws Throwable {
        AbstractCatalog oaiCatalog;
        String oaiCatalogClassName = properties.getProperty("AbstractCatalog.oaiCatalogClassName");
        String recordFactoryClassName = properties.getProperty("AbstractCatalog.recordFactoryClassName");
        if (oaiCatalogClassName == null) {
            throw new ClassNotFoundException("AbstractCatalog.oaiCatalogClassName is missing from properties file");
        }
        if (recordFactoryClassName == null) {
            throw new ClassNotFoundException("AbstractCatalog.recordFactoryClassName is missing from properties file");
        }
        Class oaiCatalogClass = Class.forName(oaiCatalogClassName);
        try {
            Constructor oaiCatalogConstructor;
            try {
                oaiCatalogConstructor = oaiCatalogClass.getConstructor(new Class[]{Properties.class, ServletContext.class});
                oaiCatalog = (AbstractCatalog) oaiCatalogConstructor.newInstance(properties, context);
            } catch (NoSuchMethodException e) {
                oaiCatalogConstructor = oaiCatalogClass.getConstructor(new Class[]{Properties.class});
                oaiCatalog = (AbstractCatalog) oaiCatalogConstructor.newInstance(properties);
            }

            LOGGER.debug("AbstractCatalog.factory: recordFactoryClassName=" + recordFactoryClassName);

            Class recordFactoryClass = Class.forName(recordFactoryClassName);
            Constructor recordFactoryConstructor = recordFactoryClass.getConstructor(new Class[]{Properties.class});
            oaiCatalog.recordFactory = (RecordFactory) recordFactoryConstructor.newInstance(properties);

            LOGGER.debug("AbstractCatalog.factory: recordFactory=" + oaiCatalog.recordFactory);

            String harvestable = properties.getProperty("AbstractCatalog.harvestable");
            if (harvestable != null && harvestable.equals("false")) {
                oaiCatalog.harvestable = false;
            }
            String secondsToLive = properties.getProperty("AbstractCatalog.secondsToLive");
            if (secondsToLive != null) {
                oaiCatalog.millisecondsToLive = Integer.parseInt(secondsToLive) * 1000;
            }
            String granularity = properties.getProperty("AbstractCatalog.granularity");
            for (int i = 0; granularity != null && i < VALID_GRANULARITIES.length; ++i) {
                if (granularity.equalsIgnoreCase(VALID_GRANULARITIES[i])) {
                    oaiCatalog.supportedGranularityOffset = i;
                    break;
                }
            }
            if (oaiCatalog.supportedGranularityOffset == -1) {
                oaiCatalog.supportedGranularityOffset = 0;
                LOGGER.error("AbstractCatalog.factory: Invalid or missing AbstractCatalog.granularity property. Setting value to default: " +
                        VALID_GRANULARITIES[oaiCatalog.supportedGranularityOffset]);
            }
        } catch (InvocationTargetException e) {
            LOGGER.error("Error during OAI Catalog initialization", e);
            throw e.getTargetException();
        }
        return oaiCatalog;
    }

    /**
     * Allow the database to return some Identify &lt;description&gt; elements
     *
     * @return an XML String fragment containing description elements
     */
    public String getDescriptions() {
        return null;
    }

    /**
     * Retrieve a list of schemaLocation values associated with the specified identifier.
     *
     * @param identifier the OAI identifier
     * @return a List<String> containing schemaLocation Strings
     * @throws IdDoesNotExistException The specified identifier doesn't exist.
     * @throws NoMetadataFormatsException The identifier exists, but no metadataFormats are
     * provided for it.
     * @throws OAIInternalServerError signals an http status code 500 problem
     */
    public abstract List getSchemaLocations(String identifier) throws IdDoesNotExistException, NoMetadataFormatsException, OAIInternalServerError;

    /**
     * Retrieve a list of Identifiers that satisfy the criteria parameters.
     *
     * @param from beginning date in the form of YYYY-MM-DD or null if earliest
     * date is desired.
     * @param until ending date in the form of YYYY-MM-DD or null if latest
     * date is desired.
     * @param set set name or null if no set is desired.
     * @param metadataPrefix the metadata prefix.
     * @return a Map object containing an optional "resumptionToken" key/value pair and an "headers" Map object. The "headers" Map contains OAI
     *         identifier keys with corresponding values of "true" or null depending on whether the identifier is deleted or not.
     * @throws BadArgumentException one or more of the arguments are bad.
     * @throws CannotDisseminateFormatException the requested metadataPrefix isn't supported
     * @throws NoItemsMatchException no items fit the criteria
     * @throws NoSetHierarchyException sets aren't defined for this repository
     * @throws OAIInternalServerError signals an http status code 500 problem
     */
    public abstract Map<String, Object> listIdentifiers(String from, String until, String set, String metadataPrefix)
            throws BadArgumentException, CannotDisseminateFormatException, NoItemsMatchException, NoSetHierarchyException, OAIInternalServerError;

    /**
     * Retrieve the next set of Identifiers associated with the resumptionToken
     *
     * @param resumptionToken implementation-dependent format taken from the
     * previous listIdentifiers() Map result.
     * @return a Map object containing an optional "resumptionToken" key/value pair and an "headers" Map object. The "headers" Map contains OAI
     *         identifier keys with corresponding values of "true" or null depending on whether the identifier is deleted or not.
     * @throws BadResumptionTokenException The resumptionToken is bad.
     * @throws OAIInternalServerError signals an http status code 500 problem
     */
    public abstract Map<String, Object> listIdentifiers(String resumptionToken) throws BadResumptionTokenException, OAIInternalServerError;

    /**
     * Retrieve the specified metadata for the specified identifier.
     *
     * @param identifier the OAI identifier.
     * @param metadataPrefix The metadata prefix.
     * @return the String containing the result record.
     * @throws IdDoesNotExistException The specified identifier doesn't exist.
     * @throws CannotDisseminateFormatException The identifier exists, but doesn't support the specified metadataPrefix.
     * @throws OAIInternalServerError signals an http status code 500 problem
     */
    public abstract String getRecord(String identifier, String metadataPrefix) throws IdDoesNotExistException, CannotDisseminateFormatException, OAIInternalServerError;

    /**
     * Retrieve the specified metadata for the specified identifier.
     *
     * @param identifier the OAI identifier.
     * @param metadataPrefix The metadata prefix.
     * @return the String containing the result record.
     * @throws OAIInternalServerError signals an http status code 500 problem.
     * @throws CannotDisseminateFormatException The identifier exists, but doesn't support the specified metadataPrefix.
     * @throws IdDoesNotExistException The identifier does not exist.
     */
    public String getMetadata(String identifier, String metadataPrefix) throws OAIInternalServerError, IdDoesNotExistException, CannotDisseminateFormatException {
        throw new OAIInternalServerError("You need to override AbstractCatalog.getMetadata()");
    }

    /**
     * Retrieve a list of records that satisfy the specified criteria
     *
     * @param from beginning date in the form of YYYY-MM-DD or null if earliest date is desired
     * @param until ending date in the form of YYYY-MM-DD or null if latest date is desired
     * @param set set name or null if no set is desired
     * @param metadataPrefix The metadata prefix.
     * @return a Map object containing an optional "resumptionToken" key/value pair and a "records" Iterator object.
     *         The "records" Iterator contains a set of Records objects.
     * @throws BadArgumentException one or more of the arguments are bad.
     * @throws CannotDisseminateFormatException the requested metadataPrefix isn't supported
     * @throws NoItemsMatchException no items fit the criteria
     * @throws NoSetHierarchyException sets aren't defined for this repository
     * @throws OAIInternalServerError signals an http status code 500 problem
     */
    public Map<String, Object> listRecords(String from, String until, String set, String metadataPrefix)
            throws BadArgumentException, CannotDisseminateFormatException, NoItemsMatchException, NoSetHierarchyException, OAIInternalServerError {

        LOGGER.trace("in AbstractCatalog.listRecords");

        Map<String, Object> listIdentifiersMap = listIdentifiers(from, until, set, metadataPrefix);
        String resumptionToken = (String) listIdentifiersMap.get("resumptionToken");
        Iterator<String> identifiers = (Iterator<String>) listIdentifiersMap.get("identifiers");

        Map<String, Object> listRecordsMap = new HashMap<String, Object>();
        List<String> records = new ArrayList<String>();

        while (identifiers.hasNext()) {
            String identifier = identifiers.next();
            try {
                records.add(getRecord(identifier, metadataPrefix));
            } catch (IdDoesNotExistException e) {
                throw new OAIInternalServerError("GetRecord failed to retrieve identifier '" + identifier + "'");
            }
        }
        listRecordsMap.put("records", records.iterator());
        if (resumptionToken != null) {
            listRecordsMap.put("resumptionToken", resumptionToken);
        }
        return listRecordsMap;
    }

    /**
     * Retrieve the next set of records associated with the resumptionToken
     *
     * @param resumptionToken implementation-dependent format taken from the
     * previous listRecords() Map result.
     * @return a Map object containing an optional "resumptionToken" key/value pair and a "records" Iterator object.
     *         The "records" Iterator contains a set of Records objects.
     * @throws BadResumptionTokenException The resumptionToken is bad.
     * @throws OAIInternalServerError signals an http status code 500 problem
     */
    public Map<String, Object> listRecords(String resumptionToken) throws BadResumptionTokenException, OAIInternalServerError {
        Map<String, Object> listIdentifiersMap = listIdentifiers(resumptionToken);
        resumptionToken = (String) listIdentifiersMap.get("resumptionToken");
        Iterator<String> identifiers = (Iterator<String>) listIdentifiersMap.get("identifiers");
        String metadataPrefix = (String) listIdentifiersMap.get("metadataPrefix");

        Map<String, Object> listRecordsMap = new HashMap<String, Object>();
        List<String> records = new ArrayList<String>();

        while (identifiers.hasNext()) {
            String identifier = identifiers.next();
            try {
                records.add(getRecord(identifier, metadataPrefix));
            } catch (IdDoesNotExistException e) {
                throw new OAIInternalServerError("GetRecord failed to retrieve identifier '" + identifier + "'");
            } catch (CannotDisseminateFormatException e) {
                // someone cheated
                throw new BadResumptionTokenException();
            }
        }
        listRecordsMap.put("records", records.iterator());
        if (resumptionToken != null) {
            listRecordsMap.put("resumptionToken", resumptionToken);
        }
        return listRecordsMap;
    }

    public Map<String, String> getResumptionMap(String resumptionToken) {
        return getResumptionMap(resumptionToken, -1, -1);
    }

    public Map<String, String> getResumptionMap(String resumptionToken, int completeListSize, int cursor) {
        Map<String, String> resumptionMap = null;
        if (resumptionToken != null) {
            resumptionMap = new HashMap<String, String>();
            resumptionMap.put("resumptionToken", resumptionToken);
            if (millisecondsToLive > 0) {
                Date then = new Date((new Date()).getTime() + millisecondsToLive);
                resumptionMap.put("expirationDate", ServerVerb.createResponseDate(then));
            }
            if (completeListSize >= 0) {
                resumptionMap.put("completeListSize", Integer.toString(completeListSize));
            }
            if (cursor >= 0) {
                resumptionMap.put("cursor", Integer.toString(cursor));
            }
        }
        return resumptionMap;
    }

    /** close the repository */
    public abstract void close();
}
