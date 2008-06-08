/*
 * BEGIN_HEADER - DO NOT EDIT
 * 
 * The contents of this file are subject to the terms
 * of the Common Development and Distribution License
 * (the "License").  You may not use this file except
 * in compliance with the License.
 *
 * You can obtain a copy of the license at
 * https://open-jbi-components.dev.java.net/public/CDDLv1.0.html.
 * See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * When distributing Covered Code, include this CDDL
 * HEADER in each file and include the License file at
 * https://open-jbi-components.dev.java.net/public/CDDLv1.0.html.
 * If applicable add the following below this CDDL HEADER,
 * with the fields enclosed by brackets "[]" replaced with
 * your own identifying information: Portions Copyright
 * [year] [name of copyright owner]
 */

/*
 * @(#)StringUtil.java 
 *
 * Copyright 2004-2007 Sun Microsystems, Inc. All Rights Reserved.
 * 
 * END_HEADER - DO NOT EDIT
 */

package com.sun.sql.framework.utils;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Static class that contains utility methods for searching and replacing through strings.
 *
 * @version 
 * @author Amrish K. Lal
 * @author Ahimanikya Satapathy
 * @author Girish Patil
 */
public class StringUtil {

    /** ALPHA_NUMERIC_REGEX: */
    public static final String ALPHA_NUMERIC_REGEX = "([_a-zA-Z0-9-]+)";

    /** Email regular expression. */
    public static final String EMAIL_REGEX = "([_a-z0-9-]+(\\.[_a-z0-9-]+)*@[a-z0-9-]+(\\.[a-z0-9-]+)+)";

    /** File name regex. */
    public static final String FILE_NAME_REGEX = "([_.a-zA-Z0-9-]+)";

    /*
     * Map of illegal file name chars to their acceptable counterparts for use in table
     * names
     */
    public static final String[][] FILE_TO_TABLE_MAPPINGS = new String[][] { { ".", "_"}, { ":", "_"}, { ";", "_"}, { ",", "_"}, { " ", "_"}, { "'", "_"}, { "\"", "_"},
            { "-", "_"},};

    public static HashMap formats = new HashMap();

    /** Name regex. */
    public static final String NAME_REGEX = "([ _a-zA-Z0-9-]+)";

    /** Numeric regex. */
    public static final String NUMERIC_REGEX = "([0-9]+(\\.[0-9]+)+)";

    public static final String SQL_IDENTIFIER_REGEX = "[a-zA-Z]+[_a-zA-Z0-9]*";

    /** Url regex. */
    public static final String URL_REGEX = "\\w+:\\/\\/([^:\\/]+)(:\\d+)?(\\/.*)?";

    /* Map of raw control characters to their escaped string counterparts */
    private static final String[][] CONTROL_CHAR_MAPPINGS = new String[][] { { "\r", "\\r"}, { "\n", "\\n"}, { "\t", "\\t"}};

    /*
     * Map of illegal field name chars to their acceptable counterparts for use in column
     * names
     */
    private static final String[][] FIELD_TO_COLUMN_MAPPINGS = FILE_TO_TABLE_MAPPINGS;

    private static final String[][] ILLEGAL_RUNTIMEVALUE_MAPPINGS = new String[][] { { "\r\n", ""}, { "\r", ""}, { "\n", ""}};

    private static final String FOUR_ZEROS = "0000" ;

    /** Runtime context for this class. */
    private static final String LOG_CATEGORY = StringUtil.class.getName();
    
    private static String PATTERN_STRING_AND_OR = "[^'\"a-zA-Z0-9][\\s\\)]+AND[\\s\\(]+[^'\"a-zA-Z0-9]|[^'\"a-zA-Z0-9][\\s\\)]+OR[\\s\\(]+[^'\"a-zA-Z0-9]" ;
    
    private static Pattern PATTERN_AND_OR = Pattern.compile(PATTERN_STRING_AND_OR);

    /**
     * Creates a column name from the given field String, mangling special characters as
     * necessary.
     *
     * @param victim String representing field name to mangle
     * @return mangled version of victim, suitable for use as a column name
     */
    public static String createColumnNameFromFieldName(final String victim) {
        return StringUtil.substituteFromMapping(victim.toUpperCase().trim(), FIELD_TO_COLUMN_MAPPINGS);
    }

    /**
     * Creates a single comma-delimited string from the given List of strings, in the
     * order in which they are stored. All white-space is trimmed from the ends of each
     * element in the List before concatenation.
     *
     * @param strings List of strings to be concatenated into a single delimited string
     * @return comma-delimited string containing contents from strings
     */
    public static final synchronized String createDelimitedStringFrom(List strings) {
        return StringUtil.createDelimitedStringFrom(strings, ',');
    }

    /**
     * Creates a single delimited string from the given List of strings using the given
     * character as the delimited, in the order in which they are stored. All white-space
     * is trimmed from the ends of each element in the List before concatenation.
     *
     * @param strings List of strings to be concatenated into a single delimited string
     * @param delimiter character to use as delimiter
     * @return comma-delimited string containing contents from strings
     */
    public static final synchronized String createDelimitedStringFrom(List strings, char delimiter) {
        if (strings == null || strings.size() == 0) {
            return "";
        }

        StringBuffer buf = new StringBuffer(strings.size() * 10);

        for (int i = 0; i < strings.size(); i++) {
            if (i != 0) {
                buf.append(delimiter);
            }
            buf.append(((String) strings.get(i)).trim());
        }

        return buf.toString();
    }

    /**
     * Creates a legal SQL identifier name from the given raw String, mangling special
     * characters as necessary.
     *
     * @param victim String representing field name to mangle
     * @return mangled version of victim, suitable for use as a SQL identifier
     */
    public static String createSQLIdentifier(final String victim) {
        // Remove leading spaces from name.
        String workingName = victim.toUpperCase().trim();

        // Then remove any non-alphabetic chars from the first position of the name, and
        // substitute underscores for non-alphanumeric, non-underscore characters within
        // the resulting string.
        return workingName.replaceAll("^[^A-Za-z]+", "").replaceAll("[^A-Za-z0-9_]", "_");
    }

    /**
     * Creates a List containing the comma-delimited string elements of the given String.
     *
     * @param delimitedList String containing comma-delimited elements to be populated
     *        into a List
     * @return List of elements from the comma-delimited string
     */
    public static final List createStringListFrom(String delimitedList) {
        return StringUtil.createStringListFrom(delimitedList, ',');
    }

    /**
     * Creates a List containing the comma-delimited string elements of the given String.
     *
     * @param delimitedList String containing comma-delimited elements to be populated
     *        into a List
     * @param delimiter character to use as delimiter
     * @return List of elements from the comma-delimited string
     */
    public static final List createStringListFrom(String delimitedList, char delimiter) {
        if (delimitedList == null || delimitedList.trim().length() == 0) {
            return Collections.EMPTY_LIST;
        }

        List strings = Collections.EMPTY_LIST;
        StringTokenizer tok = new StringTokenizer(delimitedList, String.valueOf(delimiter));
        if (tok.hasMoreTokens()) {
            strings = new ArrayList();
            do {
                strings.add(tok.nextToken().trim());
            } while (tok.hasMoreTokens());
        }

        return strings;
    }

    /**
     * Creates a table name from the given filename String, mangling special characters as
     * necessary.
     *
     * @param victim String representing filename to mangle
     * @return mangled version of victim, suitable for use as a table name
     */
    public static String createTableNameFromFileName(final String victim) {
        return StringUtil.substituteFromMapping(victim.toUpperCase().trim(), FILE_TO_TABLE_MAPPINGS);
    }



    /**
     * Escape non Alphabet, Digit and '.' characters.
     * @param iStr
     * @return
     */
    public static String escapeNonAlphaNumericCharacters(String iStr) {
        StringBuffer sb = new StringBuffer();
        int len = iStr.length();
        String hexStr = null;
        for (int i=0; i < len; i++) {
            if (!Character.isLetterOrDigit(iStr.charAt(i)) && (iStr.charAt(i) != '.')) {
                sb.append("_u");
                hexStr = Integer.toHexString(iStr.charAt(i));
                if (hexStr.length() < 4) {
                    sb.append(FOUR_ZEROS.substring(0, 4 - hexStr.length()));
                }
                sb.append(hexStr);
            }else {
                sb.append(iStr.charAt(i));
            }

        }
        return sb.toString();
    }

    /**
     * Replaces any unescaped control characters ("\r", "\n", etc.) with their escaped
     * string versions.
     *
     * @param raw String to containing unescaped control characters to process
     * @return String with escaped control characters
     */
    public static String escapeControlChars(String raw) {
        return StringUtil.substituteFromMapping(raw, CONTROL_CHAR_MAPPINGS, false);
    }

    /**
     * Replaces characters that are not allowed in a Java style string literal with their
     * escape characters. Specifically quote ("), single quote ('), new line (\n),
     * carriage return (\r), and backslash (\), and tab (\t) are escaped.
     *
     * @param s String to be escaped
     * @return escaped String
     */
    public static String escapeJavaLiteral(String s) {
        int length = s.length();
        int newLength = length;

        // first check for characters that might
        // be dangerous and calculate a length
        // of the string that has escapes.
        for (int i = 0; i < length; i++) {
            char c = s.charAt(i);

            switch (c) {
                case '\"':
                case '\'':
                case '\n':
                case '\r':
                case '\t':
                case '\\':
                    newLength += 1;
                    break;
            }
        }

        if (length == newLength) {
            // nothing to escape in the string
            return s;
        }

        StringBuffer sb = new StringBuffer(newLength);

        for (int i = 0; i < length; i++) {
            char c = s.charAt(i);

            switch (c) {
                case '\"':
                    sb.append("\\\"");
                    break;
                case '\'':
                    sb.append("\\\'");
                    break;
                case '\n':
                    sb.append("\\n");
                    break;
                case '\r':
                    sb.append("\\r");
                    break;
                case '\t':
                    sb.append("\\t");
                    break;
                case '\\':
                    sb.append("\\\\");
                    break;
                default:
                    sb.append(c);
            }
        }

        return sb.toString();
    }

    /**
     * Replaces characters that may be confused by an SQL parser with their equivalent
     * escape characters.
     * <p>
     * Any data that will be put in an SQL query should be be escaped. This is especially
     * important for data that comes from untrusted sources such as Internet users.
     * </p>
     * <p>
     * For example if you had the following SQL query: <br>
     * <code>"SELECT  FROM addresses WHERE name='" + name + "' AND private='N'"
     * </code>
     * <br>
     * Without this function a user could give <code>" OR 1=1 OR ''='"</code> as their
     * name causing the query to be: <br>
     * <code>"SELECT  FROM addresses WHERE name='' OR 1=1 OR ''=''
     * AND private='N'"</code>
     * <br>
     * which will give all addresses, including private ones. <br>
     * Correct usage would be: <br>
     * <code>"SELECT  FROM addresses WHERE name='" +
     * StringHelper.escapeSQL(name) + "' AND private='N'"</code>
     * <br>
     * </p>
     * <p>
     * Another way to avoid this problem is to use a PreparedStatement with appropriate
     * placeholders.
     * </p>
     *
     * @param s String to be escaped
     * @return escaped String
     */
    public static String escapeSQL(String s) {
        int length = s.length();
        int newLength = length;

        // first check for characters that might
        // be dangerous and calculate a length
        // of the string that has escapes.
        for (int i = 0; i < length; i++) {
            char c = s.charAt(i);

            switch (c) {
                case '\\':
                case '\"':
                case '\'':
                case '0':
                    newLength += 1;
                    break;
            }
        }

        if (length == newLength) {
            // nothing to escape in the string
            return s;
        }

        StringBuffer sb = new StringBuffer(newLength);

        for (int i = 0; i < length; i++) {
            char c = s.charAt(i);

            switch (c) {
                case '\\':
                    sb.append("\\\\");
                    break;
                case '\"':
                    sb.append("\\\"");
                    break;
                case '\'':
                    sb.append("\\\'");
                    break;
                case '0':
                    sb.append("\\0");
                    break;
                default:
                    sb.append(c);
            }
        }

        return sb.toString();
    }

    /**
     * Gets Map of known format types and their associated regular expressions.
     *
     * @return Map of format names to regular expressions.
     */
    public static Map getFormats() {
        if (formats.isEmpty()) {
            formats.put("numeric", NUMERIC_REGEX);
            formats.put("alphanumeric", ALPHA_NUMERIC_REGEX);
            formats.put("email", EMAIL_REGEX);
            formats.put("name", NAME_REGEX);
            formats.put("url", URL_REGEX);
            formats.put("filename", FILE_NAME_REGEX);
            formats.put("sqlidentifier", SQL_IDENTIFIER_REGEX);
        }

        return formats;
    }

    /**
     * StringUtil.getInt parses a string for a numeric value. If the string cannot be
     * converted to a numeric value, returns Integer.MIN_VALUE
     *
     * @param type type for which int type to be returned
     * @return int of for String.
     */
    public static int getInt(String type) {
        if (StringUtil.isNullString(type) == false) {
            try {
                return Integer.parseInt(type.trim());
            } catch (NumberFormatException ignore) {
                Logger.print(Logger.DEBUG, LOG_CATEGORY, ignore.toString());
            }
        }

        return Integer.MIN_VALUE;
    }

    /**
     * Indicates whether the given String references are identical - i.e., if one and two
     * are both null or contain the same sequence of characters, this method returns true.
     *
     * @param one first String to compare
     * @param two second String to compare
     * @return true if both one and two either (1) have the same sequence of characters,
     *         or (2) both reference <code>null</code>; false otherwise.
     */
    public static boolean isIdentical(String one, String two) {
        // Same object being compared...always true.
        if (one == two) {
            return true;
        }

        return (one == null) ? (two == null) : (two != null && one.compareTo(two) == 0);
    }


    /**
     * Indicates whether the given String references are identical - i.e., if one and two
     * are both null or contain the same sequence of characters, this method returns true.
     *
     * @param one first String to compare
     * @param two second String to compare
     * @return true if both one and two either (1) have the same sequence of characters,
     *         or (2) both reference <code>null</code>; false otherwise.
     */
    public static boolean isIdenticalIgnoreCase(String one, String two) {
        // Same object being compared...always true.
        if (one == two) {
            return true;
        }

        return (one == null) ? (two == null) : (two != null && one.compareToIgnoreCase(two) == 0);
    }

    /**
     * Indicates whether the given String references are identical - i.e., if one and two
     * are both null or contain the same sequence of characters, this method returns true.
     *
     * @param one
     * @param two
     * @param emptStringEqualsNull
     * @return true if both one and two either (1) have the same sequence of characters,
     *         or (2) both reference <code>null</code>; false otherwise.
     */
    public static boolean isIdentical(String one, String two, boolean emptStringEqualsNull) {
        String empty = "";

        if (emptStringEqualsNull) {
            if (one == null) {
                one = empty;
            }

            if (two == null) {
                two = empty;
            }
        }

        return isIdentical(one, two);
    }

    /**
     * Indicates whether a string is null or empty.
     *
     * @param str string to chec for null.
     * @return true if string is null or blank, else false.
     */
    public static boolean isNullString(String str) {
        return (str == null || str.trim().length() == 0);
    }

    /**
     * Validates the given String with the given regular expression.
     * <ul>
     * <li>INTEGER_REGEX = "-?[0-9]+"</li>
     * <li>FLOAT_REGEX = "-?[0-9]+\\.[0-9]+"</li>
     * </ul>
     *
     * @param str String representing a string to validate.
     * @param regexp String representing a regular expression.
     * @return true if variable name is valid, else false.
     */
    public static boolean isValid(String str, String regexp) {
        if (isNullString(str) || isNullString(regexp)) {
            return false;
        }

        try {
            return str.matches(regexp);
        } catch (Exception e) {
            Logger.print(Logger.DEBUG, LOG_CATEGORY, e.toString());
        }
        return false;
    }

    /**
     * Normalizes directory separators in the given String to use the UNIX standard
     * forward-slash character ('/') rather than the Microsoft standard back-slash
     * character ('\'). Normalization occurs only if the local File implementation
     * indicates that a back-slash is the standard directory separator.
     *
     * @param rawDirectoryPath String representing directory path to be normalized
     * @return normalized version of <code>rawDirectoryPath</code>
     */
    public static String normalizeDirSeparatorsToUNIX(String rawDirectoryPath) {
        if ("\\".equals(File.separator)) {
            return rawDirectoryPath.replaceAll("\\\\", "/");
        }

        return rawDirectoryPath;
    }

    /**
     * Removes the IllegalRumtimeValueChars from the rawString
     *
     * @param rawString to be cleaned up
     * @return String string without any special characters
     */
    public static String removeIllegalRuntimeValueChars(String rawString) {
        return StringUtil.substituteFromMapping(rawString, ILLEGAL_RUNTIMEVALUE_MAPPINGS);
    }

    /**
     * Replaces variables in the given string with values from the given Map, assuming
     * that they are delimited with curly braces, e.g., {foo-name}.
     *
     * @param str String containing tokens.
     * @param pairs instance values associated with tokens(key)
     * @return resultant string
     */
    public static String replace(String str, Map pairs) {
        if (isNullString(str) || pairs == null || pairs.size() == 0) {
            return str;
        }

        try {
            String result = str;

            Object[] keys = pairs.keySet().toArray();
            for (int i = 0; i < keys.length; i++) {
                String regexp = "(\\{" + (String) keys[i] + "\\})";
                result = replaceAll(result, (String) pairs.get(keys[i]), regexp);
            }

            return result;
        } catch (Exception e) {
            Logger.print(Logger.DEBUG, LOG_CATEGORY, e.toString());
            return (str);
        }
    }

    /**
     * Replaces variables in the given string with values from the given Map, using the
     * given regular expression as a search key.
     *
     * @param str String containing tokens.
     * @param pairs instance values associated with tokens(key)
     * @param regExpPrefix regular expression prefix to search for.
     * @return resultant string
     */
    public static String replace(String str, Map pairs, String regExpPrefix) {
        if (isNullString(str) || pairs == null || pairs.size() == 0 || isNullString(regExpPrefix)) {
            return str;
        }

        try {
            String result = str;

            Object[] keys = pairs.keySet().toArray();
            for (int i = 0; i < keys.length; i++) {
                String regexp = "(\\" + regExpPrefix + (String) keys[i] + ")";
                result = replaceAll(result, (String) pairs.get(keys[i]), regexp);
            }

            return result;
        } catch (Exception e) {
            Logger.print(Logger.DEBUG, LOG_CATEGORY, e.toString());
            return (str);
        }
    }

    /**
     * Replaces all occurrences of a substring which match the given regular expression in
     * the given source String with the given String value.
     *
     * @param strToReplace String containing tokens.
     * @param strReplaceWith String to replace with.
     * @param regexp Regular expression to match with
     * @return resultant string
     */
    public static String replaceAll(String strToReplace, String strReplaceWith, String regexp) {
        Pattern pattern = Pattern.compile(regexp, Pattern.CASE_INSENSITIVE | Pattern.MULTILINE);
        Matcher matcher = pattern.matcher(strToReplace);
        return matcher.replaceAll(escapeJavaRegexpChars(strReplaceWith));
    }

    /**
     * Replaces the first occurrence of a substring which matches the given regular
     * expression in the given source String with the given String value.
     *
     * @param strToReplace String containing tokens.
     * @param strReplaceWith String to replace with.
     * @param regexp Regular expression to match with
     * @return resultant string
     */
    public static String replaceFirst(String strToReplace, String strReplaceWith, String regexp) {
        Pattern pattern = Pattern.compile(regexp, Pattern.CASE_INSENSITIVE | Pattern.MULTILINE);
        Matcher matcher = pattern.matcher(strToReplace);
        return matcher.replaceFirst(escapeJavaRegexpChars(strReplaceWith));
    }

    public static String replaceTagString(String tagString, String line, String replacement) {
    	try
    	{
			int b = line.indexOf(tagString);
			int e = b + tagString.length();
			String begin = line.substring(0, b);
			String end = line.substring(e);
			return begin + replacement + end;
    	}catch(StringIndexOutOfBoundsException ex){
    		return null;
    	}
	}
    /**
     * The replaceInString method is used to replace a string within a string with a
     * substitute string.
     *
     * @param originalString is the string requiring replacements.
     * @param victim is a victim string.
     * @param replacement is a replacement string.
     * @return String after replacement.
     */
    public static String replaceInString(String originalString, String victim, String replacement) {
        return replaceInString(originalString, new String[] { victim}, new String[] { replacement});
    }

    /**
     * The replaceInString method is used to replace a list of strings within a string
     * with a list of substitute strings.
     *
     * @param originalString is the string requiring replacements.
     * @param victims is an array of string victims.
     * @param replacements is an array of corresponding replacements.
     * @return String after replacement.
     */
    public static String replaceInString(String originalString, String[] victims, String[] replacements) {

        StringBuffer resultBuffer = new StringBuffer();
        boolean bReplaced = false;

        // For all characters in the original string
        for (int charPosition = 0; charPosition < originalString.length(); charPosition++) {

            // Walk through all the replacement candidates.
            for (int nSelected = 0; !bReplaced && (nSelected < victims.length); nSelected++) {

                // If charPosition designates a replacement.
                if (originalString.startsWith(victims[nSelected], charPosition)) {

                    // Add the new replacement.
                    resultBuffer.append(replacements[nSelected]);

                    // Mark this position as a replacement.
                    bReplaced = true;

                    // Step over the replaced string.
                    charPosition += victims[nSelected].length() - 1;
                }
            }

            if (!bReplaced) {
                resultBuffer.append(originalString.charAt(charPosition));
            } else {
                // Reset for the next character.
                bReplaced = false;
            }
        }

        // Return the result as a string
        return resultBuffer.toString();
    }

    /**
     * Replaces characters or groups of characters in the given string using the given
     * mapping of Strings in the order in which they are defined.
     *
     * @param raw String to process
     * @param mappings Array of String arrays of the form {rawString, cookedString}
     * @return processed String with substitutions made
     */
    public static String substituteFromMapping(final String raw, String[][] mappings) {
        return StringUtil.substituteFromMapping(raw, mappings, false);
    }

    /**
     * Replaces any escaped control characters ("\\r", "\\n", etc.) with their actual
     * control characters.
     *
     * @param cooked String containing escaped control characters to process
     * @return String with unescaped control characters
     */
    public static String unescapeControlChars(String cooked) {
        return StringUtil.substituteFromMapping(cooked, CONTROL_CHAR_MAPPINGS, true);
    }

    /**
     * @param strToReplace
     * @return
     */
    public static String escapeJavaRegexpChars(String rawString) {
        String cookedString = null;

        if (rawString != null) {
        	// Escape \            
            cookedString = rawString.replaceAll("\\\\", "\\\\\\\\");        	
        	// Escape $
            cookedString = cookedString.replaceAll("\\$", "\\\\\\$");            
        	// Escape ?
            cookedString = cookedString.replaceAll("\\?", "\\\\\\?");
        }

        return cookedString;
    }

    /**
     * Replaces characters or groups of characters in the given string using the given
     * mapping of Strings, optionally reversing the sense of the mappings defined in the
     * given array. Mappings are applied in the order in which they are defined.
     *
     * @param raw String to process
     * @param mappings Array of String arrays of the form {rawString, cookedString}
     * @param useReverseMap if true, programmatically reverse the order of each entry in
     *        <code>mappings</code>, i.e., map from cookedString to rawString
     * @return processed String with substitutions made
     */
    private static String substituteFromMapping(final String raw, String[][] mappings, boolean useReverseMap) {
        String cooked = raw;

        if (mappings != null) {
            int toggle = useReverseMap ? 0 : 1;
            for (int i = 0; i < mappings.length; i++) {
                cooked = replaceInString(cooked, mappings[i][1 - toggle], mappings[i][toggle]);
            }
        }

        return cooked;
    }
    
    /**
     * Returns String by inserting <b>markupString</b> string before AND and OR logical operators.
     * @return
     */
    public static String insertStringBeforeLogicalOperators(String origStr, String markupString){
    	if ((origStr == null) || (markupString == null)){
    		return origStr;
    	}
    	
    	StringBuffer sb = new StringBuffer(origStr);
    	Matcher matcher = PATTERN_AND_OR.matcher(origStr);
    	int toInsertIndex = -1;
    	int prevInsertedStrLength = 0;
    	String matchedOperator = null;
    	
    	while(matcher.find()) {
    		matchedOperator = matcher.group();
    		toInsertIndex = matchedOperator.indexOf("AND");
    		if (toInsertIndex < 0){
    			toInsertIndex = matchedOperator.indexOf("OR");
    		}
    		
    		if (toInsertIndex != -1){
    			toInsertIndex += matcher.start();
    			sb.insert(toInsertIndex + prevInsertedStrLength, markupString);
    			prevInsertedStrLength += markupString.length();
    		}
    		
    		toInsertIndex = -1;    		
    	}
    	return sb.toString();
    }
}
