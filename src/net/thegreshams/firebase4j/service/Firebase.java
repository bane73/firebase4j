package net.thegreshams.firebase4j.service;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringWriter;
import java.io.Writer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;


public class Firebase {
	
	protected static final Logger 			LOGGER 					= Logger.getRootLogger();
	
	public static final String				FIREBASE_API_JSON_EXTENSION
																	= ".json";
	

	
///////////////////////////////////////////////////////////////////////////////
//
// PROPERTIES & CONSTRUCTORS
//
///////////////////////////////////////////////////////////////////////////////
	
	
	private final String baseUrl;
	
	
	public Firebase( String baseUrl ) throws FirebaseException {

		if( baseUrl == null || baseUrl.trim().isEmpty() ) {
			String msg = "baseUrl cannot be null or empty; was: '" + baseUrl + "'";
			LOGGER.error( msg );
			throw new FirebaseException( msg );
		}
		this.baseUrl = baseUrl.trim();
		LOGGER.info( "intialized with base-url: " + this.baseUrl );
	}

	
	
///////////////////////////////////////////////////////////////////////////////
//
// STATIC PUBLIC API
//
///////////////////////////////////////////////////////////////////////////////


	/**
	 * Creates a json-string representing the data provided by the map.
	 * 
	 * @param dataMap; can be null/empty, but will result in a null result;
	 * 			otherwise, must be Strings mapped to arbitrary Objects.
	 * @return the json-string representing the data
	 * @throws FirebaseException if there was an error converting the map-data into a json-string
	 */
	public static String GET_JSON_STRING_FROM_MAP( Map<String, Object> dataMap ) throws FirebaseException {
	
		/* NOTE: per Jackson-dox, the map must be of type <String, Object> */
		
		if( dataMap == null || dataMap.isEmpty() ) {
			LOGGER.info( "cannot convert data from map into json when map is null/empty" );
			return null;
		}
		
		Writer writer = new StringWriter();		
		try {
		
			ObjectMapper mapper = new ObjectMapper();
			mapper.writeValue( writer, dataMap );
			
		} catch( Throwable t ) {
			
			String msg = "unable to convert data from map into json: " + dataMap.toString();
			LOGGER.warn( msg );
			throw new FirebaseException( msg );
			
		}
		
		return writer.toString();
	}

	/**
	 * Creates a map represented by the json-data provided.
	 * 
	 * @param jsonResponse; can be null/empty, but will result in a null result;
	 * @return Strings mapped to arbitrary Objects
	 * 
	 * @throws FirebaseException if there was an error converting the json-string into map-data
	 */
	/* 'unchecked' because Jackson-dox state that a JSON-Object will always return as Map<String, Object>
	 * http://wiki.fasterxml.com/JacksonDataBinding
	 */	
	@SuppressWarnings("unchecked")
	public static Map<String, Object> GET_JSON_STRING_AS_MAP( String jsonResponse ) throws FirebaseException {
		
		if( jsonResponse == null || jsonResponse.trim().isEmpty() ) {
			LOGGER.warn( "jsonResponse was null/empty, returning empty map; was: '" + jsonResponse + "'" );
			return new HashMap<String, Object>();
		}
		jsonResponse = jsonResponse.trim();
		
		
		Map<String, Object> result = null;		
		try {
			
			ObjectMapper mapper = new ObjectMapper();
			Object o = mapper.readValue( jsonResponse, Object.class );
			if( o instanceof Map ) {
				result = (Map<String, Object>) o;
			}
			
		} catch( Throwable t ) {
			
			String msg = "unable to map json-response: " + jsonResponse; 
			LOGGER.error( msg );
			throw new FirebaseException( msg, t );
			
		}
		
		return result;
	}

	
	
///////////////////////////////////////////////////////////////////////////////
//
// PUBLIC API
//
///////////////////////////////////////////////////////////////////////////////
	

	/**
	 * GETs data from the root-url.
	 * 
	 * @return data
	 * @throws FirebaseException
	 */
	public Map<String, Object> get() throws FirebaseException {
		return this.get( null );
	}
	
	/** GETs data from the provided-path relative to the root-url.
	 * 
	 * @param path -- if null/empty, refers to root-url
	 * @return data
	 * @throws FirebaseException
	 */
	public Map<String, Object> get( String path ) throws FirebaseException {
		
		if( path != null && !path.startsWith( "/" ) ) {
			path = "/" + path;
		}
		if( path == null ) {
			path = "";
		}
		
		String jsonResponse = this.getJsonData( this.baseUrl + path + FIREBASE_API_JSON_EXTENSION );
		Map<String, Object> result = GET_JSON_STRING_AS_MAP( jsonResponse );
		
		return result;
	}
	
	/** 
	 * PUTs data to the root-url (ie: creates or overwrites).
	 * If there is already data at the root-url, this data overwrites it.
	 * If data is null/empty, any data existing at the root-url is deleted.
	 *  
	 * @param jsonData -- can be null/empty
	 * @throws FirebaseException 
	 */
	public Map<String, Object> put( String jsonData ) throws FirebaseException {
		return this.put( null, jsonData );
	}
	
	/**
	 * PUTs data to the provided-path relative to the root-url (ie: creates or overwrites)
	 * If there is already data at the path, this data overwrites it.
	 * If data is null/empty, any data existing at the path is deleted.
	 * 
	 * @param path -- if null/empty, refers to root-url
	 * @param jsonData -- can be null/empty
	 * @throws FirebaseException 
	 */
	public Map<String, Object> put( String path, String jsonData ) throws FirebaseException {
		
		if( path != null && !path.startsWith( "/" ) ) {
			path = "/" + path;
		}
		if( path == null ) {
			path = "";
		}
		
		String jsonResponse = this.putJsonData( this.baseUrl + path + FIREBASE_API_JSON_EXTENSION, jsonData );
		Map<String, Object> result = GET_JSON_STRING_AS_MAP( jsonResponse );
		
		return result;
	}
	
	/**
	 * POSTs data to the provided-path relative to the root-url (ie: modifies)
	 * Note: the Firebase API calls this 'PUSH'; rather than standard POST-behavior, this method will 
	 * 			insert the data into the provided-path but will be associated with a Firebase-assigned
	 * 			id.  
	 * 
	 * @param path
	 * @throws FirebaseException 
	 */
	public Map<String, Object> post( String path, String jsonData ) throws FirebaseException {

		if( path != null && !path.startsWith( "/" ) ) {
			path = "/" + path;
		}
		if( path == null ) {
			path = "";
		}
		
		String jsonResponse = this.postJsonData( this.baseUrl + path + FIREBASE_API_JSON_EXTENSION, jsonData );
		Map<String, Object> result = GET_JSON_STRING_AS_MAP( jsonResponse );
		
		return result;
	}
	
	public Map<String, Object> delete( String path ) throws FirebaseException {

		if( path != null && !path.startsWith( "/" ) ) {
			path = "/" + path;
		}
		if( path == null ) {
			path = "";
		}
		
		String jsonResponse = this.deleteJsonData( this.baseUrl + path + FIREBASE_API_JSON_EXTENSION );
		Map<String, Object> result = GET_JSON_STRING_AS_MAP( jsonResponse );
		
		return result;
	}
	

	
///////////////////////////////////////////////////////////////////////////////
//
// PRIVATE API
//
///////////////////////////////////////////////////////////////////////////////

	private String deleteJsonData( String deleteUrl ) throws FirebaseException {

		if( deleteUrl == null || deleteUrl.trim().isEmpty() ) {
			String msg = "deleteUrl cannot be null/empty; was: '" + deleteUrl + "'";
			LOGGER.warn( msg );
			throw new FirebaseException( msg );
		}
		deleteUrl = deleteUrl.trim();
		
		StringBuilder result = new StringBuilder();
		InputStream is = null;
		try {
		
			HttpClient client = new DefaultHttpClient();
			HttpDelete request = new HttpDelete( deleteUrl );
		    HttpResponse response = client.execute( request );
		    HttpEntity responseEntity = response.getEntity();

			Writer writer = new StringWriter();

/*BJG*/ System.out.println( "\n\nSuccess? " + response.getStatusLine().getReasonPhrase() );
/*BJG*/ System.out.println( "Code: " + response.getStatusLine().getStatusCode() + "\n\n" );

			if( responseEntity != null ) {
				is = responseEntity.getContent();
				char[] buffer = new char[1024];
				Reader reader = new BufferedReader( new InputStreamReader( is, "UTF-8" ) );
				int n;
				while( (n=reader.read(buffer)) != -1 ) {
					writer.write( buffer, 0, n );
				}
			}
			result.append( writer.toString() );
		    
		} catch( Throwable t ) {
			
			String msg = "unable to perform DELETE-request(" + deleteUrl + ")";
			LOGGER.error( msg );
			throw new FirebaseException( msg, t );
			
		} finally {
			
			// apache http-components states we should always try and close the input-stream
			if( is != null ) {
				try {
					is.close();
				} catch( IOException e ) {
					LOGGER.warn( "error closing input-stream", e );
				}
			}
			
		}
		
		LOGGER.debug( "response-data of DELETE-request(" + deleteUrl + ") was: " + result.toString() );
		
		return result.toString();
	}
	
	private String postJsonData( String postUrl, String jsonData ) throws FirebaseException {

		if( postUrl == null || postUrl.trim().isEmpty() ) {
			String msg = "postUrl cannot be null/empty; was: '" + postUrl + "'";
			LOGGER.warn( msg );
			throw new FirebaseException( msg );
		}
		postUrl = postUrl.trim();
		
		StringBuilder result = new StringBuilder();
		InputStream is = null;
		try {
			
			HttpClient client = new DefaultHttpClient();
			HttpPost request = new HttpPost( postUrl );
			StringEntity entity = new StringEntity( jsonData );
			request.setEntity( entity );
			HttpResponse response = client.execute( request );
			HttpEntity responseEntity = response.getEntity();
			
			Writer writer = new StringWriter();

/*BJG*/ System.out.println( "\n\nSuccess? " + response.getStatusLine().getReasonPhrase() );
/*BJG*/ System.out.println( "Code: " + response.getStatusLine().getStatusCode() + "\n\n" );

			if( responseEntity != null ) {
				is = responseEntity.getContent();
				char[] buffer = new char[1024];
				Reader reader = new BufferedReader( new InputStreamReader( is, "UTF-8" ) );
				int n;
				while( (n=reader.read(buffer)) != -1 ) {
					writer.write( buffer, 0, n );
				}
			}
			result.append( writer.toString() );
			
		} catch( Throwable t ) {
			
			String msg = "unable to perform POST-request(" + postUrl + ")";
			LOGGER.error( msg );
			throw new FirebaseException( msg, t );
			
		} finally {
			
			// apache http-components states we should always try and close the input-stream
			if( is != null ) {
				try {
					is.close();
				} catch( IOException e ) {
					LOGGER.warn( "error closing input-stream", e );
				}
			}
			
		}
		
		LOGGER.debug( "response-data of POST-request(" + postUrl + ") was: " + result.toString() );
		
		return result.toString();
	}
	
	private String putJsonData( String putUrl, String jsonData ) throws FirebaseException {
		
		if( putUrl == null || putUrl.trim().isEmpty() ) {
			String msg = "putUrl cannot be null/empty; was: '" + putUrl + "'";
			LOGGER.warn( msg );
			throw new FirebaseException( msg );
		}
		putUrl = putUrl.trim();
		
		StringBuilder result = new StringBuilder();
		InputStream is = null;
		try {
			
			HttpClient client = new DefaultHttpClient();
			HttpPut request = new HttpPut( putUrl );
			StringEntity entity = new StringEntity( jsonData );
			request.setEntity( entity );
			HttpResponse response = client.execute( request );
			HttpEntity responseEntity = response.getEntity();

			Writer writer = new StringWriter();

/*BJG*/ System.out.println( "\n\nSuccess? " + response.getStatusLine().getReasonPhrase() );
/*BJG*/ System.out.println( "Code: " + response.getStatusLine().getStatusCode() + "\n\n" );
			
			if( responseEntity != null ) {
				is = responseEntity.getContent();
				char[] buffer = new char[1024];
				Reader reader = new BufferedReader( new InputStreamReader( is, "UTF-8" ) );
				int n;
				while( (n=reader.read(buffer)) != -1 ) {
					writer.write( buffer, 0, n );
				}
			}
			result.append( writer.toString() );
			
			
		} catch( Throwable t ) {
			
			String msg = "unable to perform PUT-request(" + putUrl + ")";
			LOGGER.error( msg );
			throw new FirebaseException( msg, t );
			
		} finally {
			
			// apache http-components states we should always try and close the input-stream
			if( is != null ) {
				try {
					is.close();
				} catch( IOException e ) {
					LOGGER.warn( "error closing input-stream", e );
				}
			}
			
		}
		
		LOGGER.debug( "response-data of PUT-request(" + putUrl + ") was: " + result.toString() );
		
		return result.toString();
	}
	
	private String getJsonData( String getUrl ) throws FirebaseException {

		if( getUrl == null || getUrl.trim().isEmpty() ) {
			String msg = "getUrl cannot be null/empty; was: '" + getUrl + "'";
			LOGGER.warn( msg );
			throw new FirebaseException( msg );
		}
		getUrl = getUrl.trim();
		
		StringBuilder result = new StringBuilder();
		InputStream is = null;
		try {

			HttpClient client = new DefaultHttpClient();
			HttpGet request = new HttpGet( getUrl );			
			HttpResponse response = client.execute( request );
			HttpEntity entity = response.getEntity();

			Writer writer = new StringWriter();

/*BJG*/ System.out.println( "\n\nSuccess? " + response.getStatusLine().getReasonPhrase() );
/*BJG*/ System.out.println( "Code: " + response.getStatusLine().getStatusCode() + "\n\n" );

			if( entity != null ) {
				is = entity.getContent();
				char[] buffer = new char[1024];
				Reader reader = new BufferedReader( new InputStreamReader( is, "UTF-8" ) );
				int n;
				while( (n=reader.read(buffer)) != -1 ) {
					writer.write( buffer, 0, n );
				}
			}
			result.append( writer.toString() );
			
		} catch( Throwable t ) {
			
			String msg = "unable to perform GET-request(" + getUrl + ")";
			LOGGER.error( msg );
			throw new FirebaseException( msg, t );
			
		} finally {
			
			// apache http-components states we should always try and close the input-stream
			if( is != null ) {
				try {
					is.close();
				} catch( IOException e ) {
					LOGGER.warn( "error closing input-stream", e );
				}
			}
			
		}
		
		LOGGER.debug( "response-data of GET-request(" + getUrl + ") was: " + result.toString() );
		
		return result.toString();
	}

	
///////////////////////////////////////////////////////////////////////////////
//
// MAIN
//
///////////////////////////////////////////////////////////////////////////////
	
	public static void main(String[] args) throws FirebaseException, JsonParseException, JsonMappingException, IOException {

		
		// get the base-url (ie: 'http://gamma.firebase.com/username')
		String firebase_baseUrl = null;
		for( String s : args ) {

			if( s == null || s.trim().isEmpty() ) continue;
			if( s.trim().split( "=" )[0].equals( "baseUrl" ) ) {
				firebase_baseUrl = s.trim().split( "=" )[1];
			}
		}
		if( firebase_baseUrl == null || firebase_baseUrl.trim().isEmpty() ) {
			throw new IllegalArgumentException( "Program-argument 'baseUrl' not found but required" );
		}

		// create the firebase
		Firebase firebase = new Firebase( firebase_baseUrl );
	
		
		// "GET" (the root)
		Map<String, Object> map = firebase.get();
		if( map == null ) { 
			map = new LinkedHashMap<String, Object>();
		}
		Iterator<String> it = map.keySet().iterator();
		System.out.println( "\n\nResult of GET:" );
		while( it.hasNext() ) {
			String key = it.next();
			System.out.println( key + "->" + map.get(key) );
		}
		System.out.println("\n");
		
		// "GET" (the test-PUT)
		map = firebase.get( "test-PUT" );
		if( map == null ) { 
			map = new LinkedHashMap<String, Object>();
		}
		it = map.keySet().iterator();
		System.out.println( "\n\nResult of GET (for the test-PUT):" );
		while( it.hasNext() ) {
			String key = it.next();
			System.out.println( key + "->" + map.get(key) );
		}
		System.out.println("\n");
		
		
		// "PUT" (test-map into a sub-node off of the root)
		Map<String, Object> dataMap = new LinkedHashMap<String, Object>();
		dataMap.put( "Key_1", "This is the first value" );
		dataMap.put( "Key_2", "This is value #2" );
		Map<String, Object> dataMap2 = new LinkedHashMap<String, Object>();
		dataMap2.put( "Sub-Key1", "This is the first sub-value" );
		dataMap.put( "Key_3", dataMap2 );
		String jsonData = GET_JSON_STRING_FROM_MAP( dataMap );
		map = firebase.put( "test-PUT", jsonData );
		it = map.keySet().iterator();
		System.out.println( "\n\nResult of PUT (for the test-PUT):" );
		while( it.hasNext() ) {
			String key = it.next();
			System.out.println( key + "->" + map.get(key) );
		}
		System.out.println("\n");
		
		
		// "POST" (test-map into a sub-node off of the root)
		map = firebase.post( "test-POST", jsonData );
		it = map.keySet().iterator();
		System.out.println( "\n\nResult of POST (for the test-POST):" );
		while( it.hasNext() ) {
			String key = it.next();
			System.out.println( key + "->" + map.get(key) );
		}
		System.out.println("\n");
		
		
		// "DELETE"
		map = firebase.delete( "test-POST" );
		it = map.keySet().iterator();
		System.out.println( "\n\nResult of DELETE (for the test-POST):" );
		while( it.hasNext() ) {
			String key = it.next();
			System.out.println( key + "->" + map.get(key) );
		}
		System.out.println( "\n" );
	}
	
}





