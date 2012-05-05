package net.thegreshams.firebase4j.service;

import org.apache.log4j.Logger;


public class Firebase {
	
	protected static final Logger LOGGER = Logger.getRootLogger();
	

	
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
// PUBLIC API
//
///////////////////////////////////////////////////////////////////////////////

	
	
	
	

	
///////////////////////////////////////////////////////////////////////////////
//
// MAIN
//
///////////////////////////////////////////////////////////////////////////////
	
	public static void main(String[] args) throws FirebaseException {

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
	
		Firebase firebase = new Firebase( firebase_baseUrl );
	}
	
}
