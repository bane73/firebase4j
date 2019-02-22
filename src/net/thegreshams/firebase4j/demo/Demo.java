package net.thegreshams.firebase4j.demo;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

import net.thegreshams.firebase4j.error.FirebaseException;
import net.thegreshams.firebase4j.error.JacksonUtilityException;
import net.thegreshams.firebase4j.model.FirebaseResponse;
import net.thegreshams.firebase4j.service.Firebase;

import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;

public class Demo {


	public static void main(String[] args) throws FirebaseException, JsonParseException, JsonMappingException, IOException, JacksonUtilityException {

		
		// get the base-url (ie: 'http://gamma.firebase.com/username')
		String firebase_baseUrl = null;

		// get the api-key (ie: 'tR7u9Sqt39qQauLzXmRycXag18Z2')
		String firebase_apiKey = null;

		for( String s : args ) {

			if( s == null || s.trim().isEmpty() ) continue;
			String[] split = s.trim().split( "=" );

			if( split[0].equals("baseUrl") ) {
				firebase_baseUrl = split[1];
			}
			else if( split[0].equals("apiKey") ) {
				firebase_apiKey = split[1];
			}


		}
		if( firebase_baseUrl == null || firebase_baseUrl.trim().isEmpty() ) {
			throw new IllegalArgumentException( "Program-argument 'baseUrl' not found but required" );
		}


		// create the firebase
		Firebase firebase = new Firebase( firebase_baseUrl );


		// "DELETE" (the fb4jDemo-root)
		FirebaseResponse response = firebase.delete();


		// "PUT" (test-map into the fb4jDemo-root)
		Map<String, Object> dataMap = new LinkedHashMap<String, Object>();
		dataMap.put( "PUT-root", "This was PUT into the fb4jDemo-root" );
		response = firebase.put( dataMap );
		System.out.println( "\n\nResult of PUT (for the test-PUT to fb4jDemo-root):\n" + response );
		System.out.println("\n");


		// "GET" (the fb4jDemo-root)
		response = firebase.get();
		System.out.println( "\n\nResult of GET:\n" + response );
		System.out.println("\n");


		// "PUT" (test-map into a sub-node off of the fb4jDemo-root)
		dataMap = new LinkedHashMap<String, Object>();
		dataMap.put( "Key_1", "This is the first value" );
		dataMap.put( "Key_2", "This is value #2" );
		Map<String, Object> dataMap2 = new LinkedHashMap<String, Object>();
		dataMap2.put( "Sub-Key1", "This is the first sub-value" );
		dataMap.put( "Key_3", dataMap2 );
		response = firebase.put( "test-PUT", dataMap );
		System.out.println( "\n\nResult of PUT (for the test-PUT):\n" + response );
		System.out.println("\n");


		// "GET" (the test-PUT)
		response = firebase.get( "test-PUT" );
		System.out.println( "\n\nResult of GET (for the test-PUT):\n" + response );
		System.out.println("\n");


		// "POST" (test-map into a sub-node off of the fb4jDemo-root)
		response = firebase.post( "test-POST", dataMap );
		System.out.println( "\n\nResult of POST (for the test-POST):\n" + response );
		System.out.println("\n");


		// "DELETE" (it's own test-node)
		dataMap = new LinkedHashMap<String, Object>();
		dataMap.put( "DELETE", "This should not appear; should have been DELETED" );
		response = firebase.put( "test-DELETE", dataMap );
		System.out.println( "\n\nResult of PUT (for the test-DELETE):\n" + response );
		response = firebase.delete( "test-DELETE");
		System.out.println( "\n\nResult of DELETE (for the test-DELETE):\n" + response );
		response = firebase.get( "test-DELETE" );
		System.out.println( "\n\nResult of GET (for the test-DELETE):\n" + response );

		// Sign Up user for Firebase's Auth Service demo (https://firebase.google.com/docs/reference/rest/auth/)
		if(firebase_apiKey != null) {

			firebase = new Firebase("https://www.googleapis.com/identitytoolkit/v3/relyingparty", false);
			firebase.addQuery("key", firebase_apiKey);

			dataMap.clear();
			dataMap.put("email", "elonmusk@tesla.com");
			dataMap.put("password", "TeslaRocks");
			dataMap.put("returnSecureToken", true);

			response = firebase.post("signupNewUser", dataMap);
			System.out.println("\n\nResult of Signing Up:\n" + response);
			System.out.println("\n");

		} else {
			System.out.println("\n\nResult of Signing Up:\n failed, because no API Key was provided.");
			System.out.println("\n");
		}

	}
	
}




