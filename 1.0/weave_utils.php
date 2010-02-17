<?php

	require_once 'weave_constants.php';
	require_once 'weave_user/' . WEAVE_AUTH_ENGINE . '.php';

	function report_problem($message, $code = 503)
	{
		$headers = array('400' => '400 Bad Request',
					'401' => '401 Unauthorized',
					'403' => '403 Forbidden',
					'404' => '404 Not Found',
					'412' => '412 Precondition Failed',
					'503' => '503 Service Unavailable');
		header('HTTP/1.1 ' . $headers{$code},true,$code);
		
		if ($code == 401)
		{
			header('WWW-Authenticate: Basic realm="Weave"');
		}
		
		exit(json_encode($message));
	}
	
	
	function fix_utf8_encoding($string)
	{
		if(mb_detect_encoding($string) == 'UTF-8')
			return $string;
		else
			return utf8_encode($string);
	}

	function get_json()
	{
		#stupid php being helpful with input data...
		$putdata = fopen("php://input", "r");
		$jsonstring = '';
		while ($data = fread($putdata,2048)) {$jsonstring .= $data;}
		$json = json_decode(fix_utf8_encoding($jsonstring), true);

		if ($json === null)
			report_problem(WEAVE_ERROR_JSON_PARSE, 400);
			
		return $json;
	}

	function validate_username($username)
	{
		if (!$username)
			return false;
		
		if (strlen($username) > 32)
			return false;
			
		return preg_match('/[^A-Z0-9._-]/i', $username) ? false : true;
	}
	
	function validate_collection($collection)
	{
		if (!$collection)
			return false;
		
		if (strlen($collection) > 32)
			return false;
			
		return preg_match('/[^A-Z0-9._-]/i', $collection) ? false : true;
	}
	

	# Gets the username and password out of the http headers, and checks them against the auth
	function verify_user($url_user)
	{
		if (!$url_user || !preg_match('/^[A-Z0-9._-]+$/i', $url_user)) 
			report_problem(WEAVE_ERROR_INVALID_USERNAME, 400);

		$auth_user = array_key_exists('PHP_AUTH_USER', $_SERVER) ? $_SERVER['PHP_AUTH_USER'] : null;
		$auth_pw = array_key_exists('PHP_AUTH_PW', $_SERVER) ? $_SERVER['PHP_AUTH_PW'] : null;
	
		if (is_null($auth_user) || is_null($auth_pw)) 
		{
			/* CGI/FCGI auth workarounds */
			$auth_str = null;
			if (array_key_exists('Authorization', $_SERVER))
				/* Standard fastcgi configuration */
				$auth_str = $_SERVER['Authorization'];
			else if (array_key_exists('AUTHORIZATION', $_SERVER))
				/* Alternate fastcgi configuration */
				$auth_str = $_SERVER['AUTHORIZATION'];
			else if (array_key_exists('HTTP_AUTHORIZATION', $_SERVER))
				/* IIS/ISAPI and newer (yet to be released) fastcgi */
				$auth_str = $_SERVER['HTTP_AUTHORIZATION'];
			else if (array_key_exists('REDIRECT_HTTP_AUTHORIZATION', $_SERVER))
				/* mod_rewrite - per-directory internal redirect */
				$auth_str = $_SERVER['REDIRECT_HTTP_AUTHORIZATION'];
			if (!is_null($auth_str)) 
			{
				/* Basic base64 auth string */
				if (preg_match('/Basic\s+(.*)$/', $auth_str)) 
				{
					$auth_str = substr($auth_str, 6);
					$auth_str = base64_decode($auth_str, true);
					if ($auth_str != FALSE) {
						$tmp = explode(':', $auth_str);
						if (count($tmp) == 2) 
						{
							$auth_user = $tmp[0];
							$auth_pw = $tmp[1];
						}
					}
				}
			}
		}

		if (!$auth_user || !$auth_pw) #do this first to avoid the cryptic error message if auth is missing
			report_problem('Authentication failed', '401');
		
		$url_user = strtolower($url_user);
		if (strtolower($auth_user) != $url_user)
			report_problem(WEAVE_ERROR_USERID_PATH_MISMATCH, 400);


		try 
		{
			$authdb = new WeaveAuthentication($url_user);
			if (!$userid = $authdb->authenticate_user(fix_utf8_encoding($auth_pw)))
				report_problem('Authentication failed', '401');
		}
		catch(Exception $e)
		{
			header("X-Weave-Backoff: 1800");
			report_problem($e->getMessage(), $e->getCode());
		}

		#set an X-Weave-Alert header if the user needs to know something
		if ($alert = $authdb->get_user_alert())
			header("X-Weave-Alert: $alert", false);

		return $userid;
	}
	
?>