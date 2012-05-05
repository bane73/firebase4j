package net.thegreshams.firebase4j.service;

import org.apache.log4j.Logger;


public class Firebase {
	
	protected static final Logger LOGGER = Logger.getRootLogger();
	
	
	private Firebase() { /* DO NOTHING */ }

	
	
///////////////////////////////////////////////////////////////////////////////
//
//PUBLIC API
//
///////////////////////////////////////////////////////////////////////////////

	
	private static String baseUrl;
	public static void setBaseUrl( String baseUrl ) throws FirebaseException {
		
		if( baseUrl == null || baseUrl.trim().isEmpty() ) {
			String msg = "baseUrl cannot be null or empty; was: '" + baseUrl + "'";
			LOGGER.error( msg );
			throw new FirebaseException( msg );
		}
		Firebase.baseUrl = baseUrl.trim();
	}
	
}
