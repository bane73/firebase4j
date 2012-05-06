package net.thegreshams.firebase4j.model;

import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.log4j.Logger;

public class FirebaseResponse {
	
	protected static final Logger LOGGER = Logger.getRootLogger();
	

	
///////////////////////////////////////////////////////////////////////////////
//
// PROPERTIES & CONSTRUCTORS
//
///////////////////////////////////////////////////////////////////////////////

	private final boolean success;
	private final int code;
	private final Map<String, Object> body;
	private final String rawBody;
	
	public FirebaseResponse( boolean success, int code, Map<String, Object> body, String rawBody ) {
		
		this.success = success;
		this.code = code;
		
		if( body == null ) {
			LOGGER.info( "body was null; replacing with empty map" );
			body = new LinkedHashMap<String, Object>();
		}
		this.body = body;
		
		if( rawBody == null ) {
			LOGGER.info( "rawBody was null; replacing with empty string" );
			rawBody = new String();
		}
		this.rawBody = rawBody.trim();
	}
	
	
	
///////////////////////////////////////////////////////////////////////////////
//
// PUBLIC API
//
///////////////////////////////////////////////////////////////////////////////

	
	/**
	 * Returns whether or not the response from the Firebase-client was successful
	 * 
	 * @return true if response from the Firebase-client was successful
	 */
	public boolean getSuccess() {
		return this.success;
	}
	
	/**
	 * Returns the HTTP status code returned from the Firebase-client
	 * 
	 * @return an integer representing an HTTP status code
	 */
	public int getCode() {
		return this.code;
	}
	
	/**
	 * Returns a map of the data returned by the Firebase-client
	 * 
	 * @return a map of Strings to Objects
	 */
	public Map<String, Object> getBody() {
		return this.body;
	}
	
	/**
	 * Returns the raw data response returned by the Firebase-client
	 * 
	 * @return a String of the JSON-response from the client
	 */
	public String getRawBody() {
		return this.rawBody;
	}
	
	@Override
	public String toString() {
		
		StringBuilder result = new StringBuilder();
		
		result.append( FirebaseResponse.class.getSimpleName() + "[ " )
				.append( "(Success:" ).append( this.success ).append( ") " )
				.append( "(Code:" ).append( this.code ).append( ") " )
				.append( "(Body:" ).append( this.body ).append( ") " )
				.append( "(Raw-body:" ).append( this.rawBody ).append( ") " )
				.append( "]" );
		
		return result.toString();
	}
	
}
