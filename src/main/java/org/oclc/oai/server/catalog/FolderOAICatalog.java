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

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Properties;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.TreeMap;
import java.util.List;

import javax.servlet.ServletContext;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.xpath.XPathAPI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

import org.oclc.oai.server.verb.BadResumptionTokenException;
import org.oclc.oai.server.verb.CannotDisseminateFormatException;
import org.oclc.oai.server.verb.IdDoesNotExistException;
import org.oclc.oai.server.verb.NoItemsMatchException;
import org.oclc.oai.server.verb.NoMetadataFormatsException;
import org.oclc.oai.server.verb.NoSetHierarchyException;
import org.oclc.oai.util.OAIUtil;


/**
 * NewFileSystemOAICatalog is an implementation of AbstractCatalog interface
 * with the data sitting in a directory on a filesystem.
 *
 * @author Jeff Young, OCLC Online Computer Library Center
 */
public class FolderOAICatalog extends AbstractCatalog {

    /** Class logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger(FolderOAICatalog.class);

    private SimpleDateFormat dateFormatter = new SimpleDateFormat();
    protected String homeDir;
    private Map<String, Object> datestampMap = new HashMap<String, Object>();
    private Map<String, Object> identifierMap = new HashMap<String, Object>();
    private Map<String, Object> setMap = new HashMap<String, Object>();
    private Map<String, Object> resumptionResults = new HashMap<String, Object>();
    private int maxListSize;
    private List<String> sets = null;
    private ServletContext context = null;

    public FolderOAICatalog(Properties properties, ServletContext context) throws IOException {
        this.context = context;
        String temp;

        dateFormatter.applyPattern("yyyy-MM-dd'T'HH:mm:ss'Z'");
        temp = properties.getProperty("NewFileSystemOAICatalog.maxListSize");
        if (temp == null) {
            throw new IllegalArgumentException("NewFileSystemOAICatalog. maxListSize is missing from the properties file");
        }
        maxListSize = Integer.parseInt(temp);
        LOGGER.debug("in NewFileSystemOAICatalog(): maxListSize=" + maxListSize);
        homeDir = properties.getProperty("NewFileSystemOAICatalog.homeDir");
        if (homeDir != null) {
            LOGGER.debug("in NewFileSystemOAICatalog(): homeDir=" + homeDir);
            File homeFile = new File(homeDir);
            int homeDirLen = homeFile.getPath().length() + 1;
            loadFileMap(homeDirLen, homeFile);
        } else {
            /* Try looking in a known location */
            Set resourcePaths = context.getResourcePaths("/WEB-INF/DATA/");
            if (resourcePaths == null || resourcePaths.size() == 0) {
                throw new IllegalArgumentException("NewFileSystemOAICatalog. homeDir is missing from the properties file");
            }
            String earliestDatestamp = loadFileMap(context, resourcePaths);
            properties.setProperty("Identify.earliestDatestamp", earliestDatestamp);
        }

        sets = getSets(properties);
    }

    private static List<String> getSets(Properties properties) {
        InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream("sets.properties");
        Properties setProps = new Properties();
        try {
            setProps.load(is);
        } catch (Exception e) {
            LOGGER.error("An Exception occured", e);
        }
        Map<String, String> treeMap = new TreeMap<String, String>();
        Enumeration propNames = setProps.propertyNames();
        while (propNames.hasMoreElements()) {
            String propertyName = (String) propNames.nextElement();
            String s = new StringBuilder("<set><setSpec>")
                    .append(OAIUtil.xmlEncode(propertyName))
                    .append("</setSpec><setName>")
                    .append(OAIUtil.xmlEncode((String) setProps.get(propertyName)))
                    .append("</setName></set>")
                    .toString();
            treeMap.put(propertyName, s);
        }
        return new ArrayList<String>(treeMap.values());
    }

    private String loadFileMap(ServletContext context, Set resourcePaths) throws IOException {
        String earliestDatestamp = "9999-12-31";
        try {
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            DocumentBuilder builder = factory.newDocumentBuilder();
            Iterator iter = resourcePaths.iterator();
            while (iter.hasNext()) {
                String resourcePath = (String) iter.next();
                if (!resourcePath.endsWith("/")) {
                    InputStream is = context.getResourceAsStream(resourcePath);
                    InputSource data = new InputSource(is);
                    Node doc = builder.parse(data);
                    is.close();

                    String datestamp = XPathAPI.eval(doc, "/record/header/datestamp").str();
                    if (datestamp.compareTo(earliestDatestamp) < 0) {
                        earliestDatestamp = datestamp;
                    }
                    String identifier = XPathAPI.eval(doc, "/record/header/identifier").str();
                    datestampMap.put(resourcePath, datestamp);
                    identifierMap.put(identifier, resourcePath);
                    NodeList setNodes = XPathAPI.selectNodeList(doc, "/record/header/setSpec");
                    for (int j = 0; j < setNodes.getLength(); ++j) {
                        Node setSpecNode = setNodes.item(j);
                        String setSpec = XPathAPI.eval(setSpecNode, "string()").str();
                        ArrayList setSpecList = (ArrayList) setMap.get(setSpec);
                        if (setSpecList == null) {
                            setSpecList = new ArrayList();
                            setMap.put(setSpec, setSpecList);
                        }
                        setSpecList.add(resourcePath);
                    }
                }
            }
        } catch (Exception e) {
            LOGGER.error("An Exception occured", e);
            throw new IOException(e.getMessage());
        }
        return earliestDatestamp;
    }

    private void loadFileMap(int homeDirLen, File currentDir) throws IOException {
        try {
            String[] list = currentDir.list();
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            DocumentBuilder builder = factory.newDocumentBuilder();
            for (int i = 0; i < list.length; ++i) {
                File child = new File(currentDir, list[i]);
                if (child.isDirectory() && !"CVS".equals(child.getName())) {
                    loadFileMap(homeDirLen, child);
                } else if (isMetadataFile(child)) {
                    String path = file2path(homeDirLen, child);
                    LOGGER.debug("parsing " + path);
                    //                 String datestamp = date2OAIDatestamp(new Date(child.lastModified()));
                    File file = localIdentifier2File(path);
                    FileInputStream fis = new FileInputStream(file);
                    InputSource data = new InputSource(fis);
                    Node doc = builder.parse(data);
                    fis.close();

                    Node datestampNode = XPathAPI.selectSingleNode(doc, "/record/header/datestamp");
                    datestampMap.put(path, XPathAPI.eval(datestampNode, "string()").str());
                    NodeList setNodes = XPathAPI.selectNodeList(doc, "/record/header/setSpec");
                    for (int j = 0; j < setNodes.getLength(); ++j) {
                        Node setSpecNode = setNodes.item(j);
                        String setSpec = XPathAPI.eval(setSpecNode,
                                "string()").str();
                        List<String> setSpecList = (List<String>) setMap.get(setSpec);
                        if (setSpecList == null) {
                            setSpecList = new ArrayList<String>();
                            setMap.put(setSpec, setSpecList);
                        }
                        setSpecList.add(path);
                    }
                }
            }
        } catch (Exception e) {
            LOGGER.error("An Exception occured", e);
            throw new IOException(e.getMessage());
        }
    }

    /**
     * Override this method if some files exist in the
     * filesystem that aren't metadata records.
     *
     * @param child the File to be investigated
     * @return true if it contains metadata, false otherwise
     */
    protected boolean isMetadataFile(File child) {
        return true;
    }

    /**
     * Override this method if you don't like the default localIdentifiers.
     *
     * @param homeDirLen the length of the home directory path
     * @param file the File object containing the native record
     * @return localIdentifier
     */
    protected String file2path(int homeDirLen, File file) {
        String fileName = file.getPath().substring(homeDirLen).replace(File.separatorChar, '/');
        return fileName;
    }

    /**
     * Override this method if you don't like the default localIdentifiers.
     *
     * @param localIdentifier the localIdentifier as parsed from the OAI identifier
     * @return the File object containing the native record
     */
    protected File localIdentifier2File(String localIdentifier) {
        String fileName = localIdentifier.replace('/', File.separatorChar);
        return new File(homeDir, fileName);
    }


    private Document getNativeRecord(String path) {
        Document nativeRecord = null;

        if (datestampMap.containsKey(path)) {
            try {
                InputStream is = context.getResourceAsStream(path);
                nativeRecord = OAIUtil.parse(is);
            } catch (Exception e) {
                LOGGER.error("An Exception occured", e);
                return null;
            }
        }
        return nativeRecord;
    }

    private List<String> getExtensionList(String localIdentifier) {
        List<String> list = new ArrayList<String>();
        Iterator iterator = datestampMap.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry entry = (Map.Entry) iterator.next();
            if (((String) entry.getKey()).startsWith(localIdentifier)) {
                list.add(((String) entry.getKey()).substring(localIdentifier.length() + 1));
            }
        }
        return list;
    }

    /**
     * Retrieve the specified metadata for the specified oaiIdentifier
     *
     * @param oaiIdentifier the OAI identifier
     * @param metadataPrefix the OAI metadataPrefix
     * @return the Record object containing the result.
     * @throws CannotDisseminateFormatException signals an http status
     * code 400 problem
     * @throws IdDoesNotExistException signals an http status code 404 problem
     */
    public String getRecord(String oaiIdentifier, String metadataPrefix) throws IdDoesNotExistException, CannotDisseminateFormatException {
        Document nativeItem = null;
        String resourcePath = (String) identifierMap.get(oaiIdentifier);

        nativeItem = getNativeRecord(resourcePath);
        if (nativeItem == null) {
            throw new IdDoesNotExistException(oaiIdentifier);
        }
        return constructRecord(nativeItem, metadataPrefix);
    }


    /**
     * Retrieve a list of schemaLocation values associated with the specified
     * oaiIdentifier.
     * <p/>
     * We get passed the ID for a record and are supposed to return a list
     * of the formats that we can deliver the record in.  Since we are assuming
     * that all the records in the directory have the same format, the
     * response to this is static;
     *
     * @param oaiIdentifier the OAI identifier
     * @return a List<String> containing schemaLocation Strings
     */
    public List<String> getSchemaLocations(String oaiIdentifier) throws IdDoesNotExistException, NoMetadataFormatsException {
        List<String> extensionList = null;
        String localIdentifier = getRecordFactory().fromOAIIdentifier(oaiIdentifier);
        extensionList = getExtensionList(localIdentifier);

        if (extensionList != null) {
            return getRecordFactory().getSchemaLocations(extensionList);
        } else {
            throw new IdDoesNotExistException(oaiIdentifier);
        }
    }


    /**
     * Retrieve a list of Identifiers that satisfy the criteria parameters
     *
     * @param from beginning date in the form of YYYY-MM-DD or null if earliest date is desired
     * @param until ending date in the form of YYYY-MM-DD or null if latest date is desired
     * @param set set name or null if no set is desired
     * @return a Map object containing an optional "resumptionToken" key/value
     *         pair and an "identifiers" Map object. The "identifiers" Map contains OAI
     *         identifier keys with corresponding values of "true" or null depending on
     *         whether the identifier is deleted or not.
     */
    public Map<String, Object> listIdentifiers(String from, String until, String set, String metadataPrefix) throws NoItemsMatchException {
        purge(); // clean out old resumptionTokens
        Map<String, Object> listIdentifiersMap = new HashMap<String, Object>();
        List<String> headers = new ArrayList<String>();
        List<String> identifiers = new ArrayList<String>();
        Iterator iterator = datestampMap.entrySet().iterator();
        int numRows = datestampMap.entrySet().size();
        int count = 0;
        ArrayList setIdentifiers = (ArrayList) setMap.get(set);
        while (count < maxListSize && iterator.hasNext()) {
            Map.Entry entryDateMap = (Map.Entry) iterator.next();
            String fileDate = (String) entryDateMap.getValue();
            String path = (String) entryDateMap.getKey();
            if (fileDate.compareTo(from) >= 0 && fileDate.compareTo(until) <= 0 && (setIdentifiers == null || setIdentifiers.contains(path))) {
                Document nativeRecord = getNativeRecord((String) entryDateMap.getKey());
                String[] header = getRecordFactory().createHeader(nativeRecord);
                headers.add(header[0]);
                identifiers.add(header[1]);
                count++;
            }
        }

        if (count == 0) {
            throw new NoItemsMatchException();
        }

        /* decide if you're done */
        if (iterator.hasNext()) {
            String resumptionId = getRSName();
            resumptionResults.put(resumptionId, iterator);

            /*****************************************************************
             * Construct the resumptionToken String however you see fit.
             *****************************************************************/
            StringBuilder resumptionTokenSb = new StringBuilder();
            resumptionTokenSb.append(resumptionId);
            resumptionTokenSb.append(":");
            resumptionTokenSb.append(Integer.toString(count));
            resumptionTokenSb.append(":");
            resumptionTokenSb.append(Integer.toString(numRows));
            resumptionTokenSb.append(":");
            resumptionTokenSb.append(metadataPrefix);
            resumptionTokenSb.append(":");
            resumptionTokenSb.append(set);

            /*****************************************************************
             * Use the following line if you wish to include the optional
             * resumptionToken attributes in the response. Otherwise, use the
             * line after it that I've commented out.
             *****************************************************************/
            listIdentifiersMap.put("resumptionMap", getResumptionMap(resumptionTokenSb.toString(), numRows, 0));
        }
        listIdentifiersMap.put("headers", headers.iterator());
        listIdentifiersMap.put("identifiers", identifiers.iterator());
        return listIdentifiersMap;
    }

    /**
     * Retrieve the next set of Identifiers associated with the resumptionToken
     *
     * @param resumptionToken implementation-dependent format taken from the previous listIdentifiers() Map result.
     * @return a Map object containing an optional "resumptionToken" key/value
     *         pair and an "identifiers" Map object. The "identifiers" Map contains OAI
     *         identifier keys with corresponding values of "true" or null depending on
     *         whether the identifier is deleted or not.
     */
    public Map<String, Object> listIdentifiers(String resumptionToken) throws BadResumptionTokenException {
        purge(); // clean out old resumptionTokens
        Map<String, Object> listIdentifiersMap = new HashMap<String, Object>();
        List<String> headers = new ArrayList<String>();
        List<String> identifiers = new ArrayList<String>();

        /**********************************************************************
         * parse your resumptionToken and look it up in the resumptionResults,
         * if necessary
         **********************************************************************/
        StringTokenizer tokenizer = new StringTokenizer(resumptionToken, ":");
        String resumptionId;
        int oldCount;
        String metadataPrefix;
        int numRows;
        String set;
        try {
            resumptionId = tokenizer.nextToken();
            oldCount = Integer.parseInt(tokenizer.nextToken());
            numRows = Integer.parseInt(tokenizer.nextToken());
            metadataPrefix = tokenizer.nextToken();
            set = tokenizer.nextToken();
        } catch (NoSuchElementException e) {
            throw new BadResumptionTokenException();
        }

        /* Get some more records from your database */
        Iterator iterator = (Iterator) resumptionResults.remove(resumptionId);
        if (iterator == null) {
            LOGGER.debug("NewFileSystemOAICatalog.listIdentifiers: reuse of old resumptionToken?");
            iterator = datestampMap.entrySet().iterator();
            for (int i = 0; i < oldCount; ++i) {
                iterator.next();
            }
        }

        /* load the headers and identifiers ArrayLists. */
        int count = 0;
        ArrayList setIdentifiers = (ArrayList) setMap.get(set);
        while (count < maxListSize && iterator.hasNext()) {
            Map.Entry entryDateMap = (Map.Entry) iterator.next();
            String path = (String) entryDateMap.getKey();
            if (setIdentifiers == null || setIdentifiers.contains(path)) {
                Document nativeRecord = getNativeRecord((String) entryDateMap.getKey());
                String[] header = getRecordFactory().createHeader(nativeRecord);
                headers.add(header[0]);
                identifiers.add(header[1]);
                count++;
            }
        }

        /* decide if you're done. */
        if (iterator.hasNext()) {
            resumptionId = getRSName();
            resumptionResults.put(resumptionId, iterator);

            /*****************************************************************
             * Construct the resumptionToken String however you see fit.
             *****************************************************************/
            StringBuilder resumptionTokenSb = new StringBuilder();
            resumptionTokenSb.append(resumptionId);
            resumptionTokenSb.append(":");
            resumptionTokenSb.append(Integer.toString(oldCount + count));
            resumptionTokenSb.append(":");
            resumptionTokenSb.append(Integer.toString(numRows));
            resumptionTokenSb.append(":");
            resumptionTokenSb.append(metadataPrefix);
            resumptionTokenSb.append(":");
            resumptionTokenSb.append(set);

            /*****************************************************************
             * Use the following line if you wish to include the optional
             * resumptionToken attributes in the response. Otherwise, use the
             * line after it that I've commented out.
             *****************************************************************/
            listIdentifiersMap.put("resumptionMap", getResumptionMap(resumptionTokenSb.toString(), numRows, oldCount));
        }

        listIdentifiersMap.put("headers", headers.iterator());
        listIdentifiersMap.put("identifiers", identifiers.iterator());
        return listIdentifiersMap;
    }


    /**
     * Utility method to construct a Record object for a specified
     * metadataFormat from a native record
     *
     * @param nativeItem native item from the dataase
     * @param metadataPrefix the desired metadataPrefix for performing the crosswalk
     * @return the <record/> String
     * @throws CannotDisseminateFormatException the record is not available
     * for the specified metadataPrefix.
     */
    private String constructRecord(Document nativeItem, String metadataPrefix) throws CannotDisseminateFormatException {
        String schemaURL = null;
        Iterator setSpecs = getSetSpecs(nativeItem);
        Iterator abouts = getAbouts(nativeItem);

        if (metadataPrefix != null) {
            if ((schemaURL = getCrosswalks().getSchemaURL(metadataPrefix)) == null) {
                throw new CannotDisseminateFormatException(metadataPrefix);
            }
        }
        return getRecordFactory().create(nativeItem, schemaURL, metadataPrefix, setSpecs, abouts);
    }

    /**
     * get an Iterator containing the setSpecs for the nativeItem
     *
     * @param nativeItem Document containing the nativeItem
     * @return an Iterator containing the list of setSpec values for this nativeItem
     */
    private Iterator getSetSpecs(Document nativeItem) {
        return null;
    }

    /**
     * get an Iterator containing the abouts for the nativeItem
     *
     * @param nativeItem Document containing the nativeItem
     * @return an Iterator containing the list of about values for this nativeItem
     */
    private Iterator getAbouts(Document nativeItem) {
        return null;
    }

    /**
     * Retrieve a list of records that satisfy the specified criteria
     *
     * @param from beginning date in the form of YYYY-MM-DD or null if earliest date is desired
     * @param until ending date in the form of YYYY-MM-DD or null if latest date is desired
     * @param set set name or null if no set is desired
     * @param metadataPrefix the OAI metadataPrefix
     * @return a Map object containing an optional "resumptionToken" key/value
     *         pair and a "records" Iterator object. The "records" Iterator contains a
     *         set of Records objects.
     */
    public Map<String, Object> listRecords(String from, String until, String set, String metadataPrefix)
            throws CannotDisseminateFormatException, NoItemsMatchException {
        purge(); // clean out old resumptionTokens
        Map<String, Object> listRecordsMap = new HashMap<String, Object>();
        List<String> records = new ArrayList<String>();
        Iterator iterator = datestampMap.entrySet().iterator();
        int numRows = datestampMap.entrySet().size();
        int count = 0;
        List<String> setIdentifiers = (List<String>) setMap.get(set);
        while (count < maxListSize && iterator.hasNext()) {
            Map.Entry entryDateMap = (Map.Entry) iterator.next();
            String fileDate = (String) entryDateMap.getValue();
            String path = (String) entryDateMap.getKey();
            if (fileDate.compareTo(from) >= 0 && fileDate.compareTo(until) <= 0
                    && (setIdentifiers == null || setIdentifiers.contains(path))) {
                Document nativeItem = getNativeRecord((String) entryDateMap.getKey());
                String record = constructRecord(nativeItem, metadataPrefix);
                records.add(record);
                count++;
            }
        }

        if (count == 0) {
            throw new NoItemsMatchException();
        }

        /* decide if you're done */
        if (iterator.hasNext()) {
            String resumptionId = getRSName();
            resumptionResults.put(resumptionId, iterator);

            /*****************************************************************
             * Construct the resumptionToken String however you see fit.
             *****************************************************************/
            StringBuilder resumptionTokenSb = new StringBuilder();
            resumptionTokenSb.append(resumptionId);
            resumptionTokenSb.append(":");
            resumptionTokenSb.append(Integer.toString(count));
            resumptionTokenSb.append(":");
            resumptionTokenSb.append(Integer.toString(numRows));
            resumptionTokenSb.append(":");
            resumptionTokenSb.append(metadataPrefix);
            resumptionTokenSb.append(":");
            resumptionTokenSb.append(set);

            /*****************************************************************
             * Use the following line if you wish to include the optional
             * resumptionToken attributes in the response. Otherwise, use the
             * line after it that I've commented out.
             *****************************************************************/
            listRecordsMap.put("resumptionMap", getResumptionMap(resumptionTokenSb.toString(), numRows, 0));
        }
        listRecordsMap.put("records", records.iterator());
        return listRecordsMap;
    }


    /**
     * Retrieve the next set of records associated with the resumptionToken
     *
     * @param resumptionToken implementation-dependent format taken from the
     * previous listRecords() Map result.
     * @return a Map object containing an optional "resumptionToken" key/value pair and a "records" Iterator object.
     *         The "records" Iterator contains a set of Records objects.
     */
    public Map<String, Object> listRecords(String resumptionToken) throws BadResumptionTokenException {
        purge(); // clean out old resumptionTokens
        Map<String, Object> listRecordsMap = new HashMap<String, Object>();
        List<String> records = new ArrayList<String>();

        /**********************************************************************
         * parse your resumptionToken and look it up in the resumptionResults,
         * if necessary
         **********************************************************************/
        StringTokenizer tokenizer = new StringTokenizer(resumptionToken, ":");
        String resumptionId;
        int oldCount;
        String metadataPrefix;
        int numRows;
        String set;
        try {
            resumptionId = tokenizer.nextToken();
            oldCount = Integer.parseInt(tokenizer.nextToken());
            numRows = Integer.parseInt(tokenizer.nextToken());
            metadataPrefix = tokenizer.nextToken();
            set = tokenizer.nextToken();
        } catch (NoSuchElementException e) {
            throw new BadResumptionTokenException();
        }

        /* Get some more records from your database */
        Iterator iterator = (Iterator) resumptionResults.remove(resumptionId);
        if (iterator == null) {
            LOGGER.debug("NewFileSystemOAICatalog.listRecords: reuse of old resumptionToken?");
            iterator = datestampMap.entrySet().iterator();
            for (int i = 0; i < oldCount; ++i) {
                iterator.next();
            }
        }

        /* load the records ArrayLists. */
        int count = 0;
        ArrayList setIdentifiers = (ArrayList) setMap.get(set);
        while (count < maxListSize && iterator.hasNext()) {
            Map.Entry entryDateMap = (Map.Entry) iterator.next();
            String path = (String) entryDateMap.getKey();
            if (setIdentifiers == null || setIdentifiers.contains(path)) {
                try {
                    Document nativeItem = getNativeRecord((String) entryDateMap.getKey());
                    String record = constructRecord(nativeItem, metadataPrefix);
                    records.add(record);
                    count++;
                } catch (CannotDisseminateFormatException e) {
                    /* the client hacked the resumptionToken beyond repair */
                    throw new BadResumptionTokenException();
                }
            }
        }

        /* decide if you're done. */
        if (iterator.hasNext()) {
            resumptionId = getRSName();
            resumptionResults.put(resumptionId, iterator);

            /*****************************************************************
             * Construct the resumptionToken String however you see fit.
             *****************************************************************/
            StringBuilder resumptionTokenSb = new StringBuilder();
            resumptionTokenSb.append(resumptionId);
            resumptionTokenSb.append(":");
            resumptionTokenSb.append(Integer.toString(oldCount + count));
            resumptionTokenSb.append(":");
            resumptionTokenSb.append(Integer.toString(numRows));
            resumptionTokenSb.append(":");
            resumptionTokenSb.append(metadataPrefix);
            resumptionTokenSb.append(":");
            resumptionTokenSb.append(set);

            /*****************************************************************
             * Use the following line if you wish to include the optional
             * resumptionToken attributes in the response. Otherwise, use the
             * line after it that I've commented out.
             *****************************************************************/
            listRecordsMap.put("resumptionMap", getResumptionMap(resumptionTokenSb.toString(), numRows, oldCount));
        }

        listRecordsMap.put("records", records.iterator());
        return listRecordsMap;
    }


    public Map<String, Object> listSets() throws NoSetHierarchyException {
        if (sets.size() == 0) {
            throw new NoSetHierarchyException();
        }
        Map<String, Object> listSetsMap = new LinkedHashMap<String, Object>();
        listSetsMap.put("sets", sets.iterator());
        return listSetsMap;
    }


    public Map<String, Object> listSets(String resumptionToken) throws BadResumptionTokenException {
        throw new BadResumptionTokenException();
    }


    /** close the repository */
    public void close() {
    }


    /** Purge tokens that are older than the time-to-live. */
    private void purge() {
        List<String> old = new ArrayList<String>();
        Date then, now = new Date();

        for (String key : resumptionResults.keySet()) {
            then = new Date(Long.parseLong(key) + getMillisecondsToLive());
            if (now.after(then)) {
                old.add(key);
            }
        }
        for (String key : old) {
            resumptionResults.remove(key);
        }
    }


    /**
     * Use the current date as the basis for the resumptiontoken
     *
     * @return a long integer version of the current time
     */
    private synchronized static String getRSName() {
        Date now = new Date();
        return Long.toString(now.getTime());
    }
}
