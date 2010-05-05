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
		global $cef;
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
			$lockout = null;
			if (defined('WEAVE_STORAGE_LOCKOUT_COUNT') && WEAVE_STORAGE_LOCKOUT_COUNT)
			{
				require_once 'lockout.php';
				$lockout = new WeaveLockout($url_user);
				if ($lockout->is_locked())
				{
					report_problem('Account locked', '401');						
				}
			}
			
			$authdb = new WeaveAuthentication($url_user);
			if (!$userid = $authdb->authenticate_user(fix_utf8_encoding($auth_pw)))
			{
				$message = new CommonEventFormatMessage('AuthFail', 'Authentication', 5, 
									array('suser' => $url_user));
				$cef->logMessage($message);

				if ($lockout)
					$lockout->increment_lockout();
					
				report_problem('Authentication failed', '401');				
			}
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

	function check_quota(&$db)
	{
		if (!defined('WEAVE_QUOTA'))
			return;
		
		if ($db->get_storage_total() > WEAVE_QUOTA)
				report_problem("Over Quota", 403); 
	}
	
	function check_timestamp($collection, &$db)
	{
		if (array_key_exists('HTTP_X_IF_UNMODIFIED_SINCE', $_SERVER) 
			&& $db->get_max_timestamp($collection) > round($_SERVER['HTTP_X_IF_UNMODIFIED_SINCE'] * 100))
				report_problem(WEAVE_ERROR_NO_OVERWRITE, 412);			
	}

	function validate_search_params()
	{
		$params = array();
		$params['parentid'] = (array_key_exists('parentid', $_GET) && mb_strlen($_GET['parentid'], '8bit') <= 64 && strpos($_GET['parentid'], '/') === false) ? $_GET['parentid'] : null;
		$params['predecessorid'] = (array_key_exists('predecessorid', $_GET) && mb_strlen($_GET['predecessorid'], '8bit') <= 64 && strpos($_GET['predecessorid'], '/') === false) ? $_GET['predecessorid'] : null;
	
		$params['newer'] = (array_key_exists('newer', $_GET) && is_numeric($_GET['newer'])) ? round($_GET['newer'] * 100) : null;
		$params['older'] = (array_key_exists('older', $_GET) && is_numeric($_GET['older'])) ? round($_GET['older'] * 100) : null;
			
		$params['sort'] = (array_key_exists('sort', $_GET) && ($_GET['sort'] == 'oldest' || $_GET['sort'] == 'newest' || $_GET['sort'] == 'index')) ? $_GET['sort'] : null;
		$params['limit'] = (array_key_exists('limit', $_GET) && is_numeric($_GET['limit']) && $_GET['limit'] > 0) ? (int)$_GET['limit'] : null;
		$params['offset'] = (array_key_exists('offset', $_GET) && is_numeric($_GET['offset']) && $_GET['offset'] > 0) ? (int)$_GET['offset'] : null;
	
		$params['ids'] = null;
		if (array_key_exists('ids', $_GET))
		{
			$params['ids'] = array();
			foreach(explode(',', $_GET['ids']) as $id)
			{
				if (mb_strlen($id, '8bit') <= 64 && strpos($id, '/') === false)
					$params['ids'][] = $id;
			}
		}
	
		$params['index_above'] = (array_key_exists('index_above', $_GET) && is_numeric($_GET['index_above']) && $_GET['index_above'] > 0) ? (int)$_GET['index_above'] : null;
		$params['index_below'] = (array_key_exists('index_below', $_GET) && is_numeric($_GET['index_below']) && $_GET['index_below'] > 0) ? (int)$_GET['index_below'] : null;
		$params['depth'] = (array_key_exists('depth', $_GET) && is_numeric($_GET['depth']) && $_GET['depth'] > 0) ? (int)$_GET['depth'] : null;
	
		return $params;
	}

	function get_source_ip()
	{
		if (array_key_exists('HTTP_X_FORWARDED_FOR', $_SERVER))
			return $_SERVER['HTTP_X_FORWARDED_FOR'];
		return $_SERVER['REMOTE_ADDR'];
	
	}
	
?>