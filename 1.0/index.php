<?php

# ***** BEGIN LICENSE BLOCK *****
# Version: MPL 1.1/GPL 2.0/LGPL 2.1
#
# The contents of this file are subject to the Mozilla Public License Version
# 1.1 (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy of the License at
# http://www.mozilla.org/MPL/
#
# Software distributed under the License is distributed on an "AS IS" basis,
# WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
# for the specific language governing rights and limitations under the
# License.
#
# The Original Code is Weave Basic Object Server
#
# The Initial Developer of the Original Code is
# Mozilla Labs.
# Portions created by the Initial Developer are Copyright (C) 2008
# the Initial Developer. All Rights Reserved.
#
# Contributor(s):
#	Toby Elliott (telliott@mozilla.com)
#   Luca Tettamanti
#
# Alternatively, the contents of this file may be used under the terms of
# either the GNU General Public License Version 2 or later (the "GPL"), or
# the GNU Lesser General Public License Version 2.1 or later (the "LGPL"),
# in which case the provisions of the GPL or the LGPL are applicable instead
# of those above. If you wish to allow use of your version of this file only
# under the terms of either the GPL or the LGPL, and not to allow others to
# use your version of this file under the terms of the MPL, indicate your
# decision by deleting the provisions above and replace them with the notice
# and other provisions required by the GPL or the LGPL. If you do not delete
# the provisions above, a recipient may use your version of this file under
# the terms of any one of the MPL, the GPL or the LGPL.
#
# ***** END LICENSE BLOCK *****

	require_once 'weave_constants.php';
	require_once 'weave_storage.php';
	require_once 'weave_authentication.php';
	require_once 'weave_basic_object.php';

	function report_problem($message, $code = 503)
	{
		$headers = array('400' => '400 Bad Request',
					'401' => '401 Unauthorized',
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
	
	
	header("Content-type: application/json");
	
	
	#get the http auth user data
	
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

	$server_time = round(microtime(1), 2);
	header("X-Weave-Timestamp: " . $server_time);
	$storage_time = $server_time * 100; #internal representation as bigint

	#Basic path extraction and validation. No point in going on if these are missing
	$path = '/';
	if (!empty($_SERVER['PATH_INFO'])) {
		$path = $_SERVER['PATH_INFO'];
	}
	else if (!empty($_SERVER['ORIG_PATH_INFO'])) {
		$path = $_SERVER['ORIG_PATH_INFO'];
	}
	$path = substr($path, 1); #chop the lead slash
	list($username, $function, $collection, $id) = explode('/', $path.'//');

	# Lowercase username before checking path
	$username = strtolower($username);
	$auth_user = strtolower($auth_user);
	
	if (!$username)
		report_problem(3, 400);

	if ($auth_user != $username)
		report_problem(5, 401);
	
	#quick check to make sure that any non-storage function calls are just using GET
	if ($function != 'storage' && $_SERVER['REQUEST_METHOD'] != 'GET')
		report_problem(1, 400);
	
	#only a get has meaning without a collection (GET returns a collection list)
	if (!$collection && $_SERVER['REQUEST_METHOD'] != 'GET')
		report_problem(1, 400);

	#storage requires a collection to have been passed in. Info requires a directive
	if (!$collection)
		report_problem(1, 400);

	#Auth the user
	try 
	{
		$authdb = get_auth_object();
		if (!$userid = $authdb->authenticate_user($auth_user, utf8_encode($auth_pw)))
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
	
	#user passes, onto actually getting the data
	if ($_SERVER['REQUEST_METHOD'] == 'GET')
	{
		try
		{
			$db = get_storage_read_object($userid, WEAVE_SHARE_DBH ? $authdb->get_connection() : null);	
		}
		catch(Exception $e)
		{
			header("X-Weave-Backoff: 1800");
			report_problem($e->getMessage(), $e->getCode());
		}
		
		if ($function == 'info')
		{
			switch ($collection)
			{
				case 'quota':
					exit(json_encode(array($db->get_storage_total(), $db->get_user_quota())));
				case 'collections':
					$collection_store = new WeaveCollectionTimestamps($userid, $db);
					$results = $collection_store->get_collection_timestamps();
					foreach ($results as $k => $v)
						$results[$k] = $results[$k]/100;
					exit(json_encode($results));
				case 'collection_counts':
					exit(json_encode($db->get_collection_list_with_counts()));
				default:
					report_problem(1, 400);
			}
		}
		elseif ($function == 'storage')
		{
			if ($id) #retrieve a single record
			{
				try
				{
					$wbo = $db->retrieve_objects($collection, $id, 1); #get the full contents of one record
				}
				catch(Exception $e)
				{
					report_problem($e->getMessage(), $e->getCode());
				}
				
				if (count($wbo) > 0)
					echo $wbo[0]->json();
				else
					report_problem("record not found", 404);
			}
			else #retrieve a batch of records. Sadly, due to potential record sizes, have the storage object stream the output...
			{
				$full = array_key_exists('full', $_GET) ? $_GET['full'] : null;
				$outputter = new WBOJsonOutput($full);
				if (array_key_exists('HTTP_ACCEPT', $_SERVER)
					&& !preg_match('/\*\/\*/', $_SERVER['HTTP_ACCEPT'])
					&& !preg_match('/application\/json/', $_SERVER['HTTP_ACCEPT']))
				{
					if (preg_match('/application\/whoisi/', $_SERVER['HTTP_ACCEPT']))
					{
						header("Content-type: application/whoisi");
						$outputter->set_format('whoisi');
					}
					elseif (preg_match('/application\/newlines/', $_SERVER['HTTP_ACCEPT']))
					{
						header("Content-type: application/newlines");
						$outputter->set_format('newlines');
					}
					
				}
	
				try 
				{
					$ids = $db->retrieve_objects($collection, null, $full, $outputter,
								array_key_exists('parentid', $_GET) ? $_GET['parentid'] : null, 
								array_key_exists('predecessorid', $_GET) ? $_GET['predecessorid'] : null, 
								array_key_exists('newer', $_GET) ? $_GET['newer'] * 100 : null, 
								array_key_exists('older', $_GET) ? $_GET['older'] * 100 : null, 
								array_key_exists('sort', $_GET) ? $_GET['sort'] : null, 
								array_key_exists('limit', $_GET) ? $_GET['limit'] : null, 
								array_key_exists('offset', $_GET) ? $_GET['offset'] : null,
								array_key_exists('ids', $_GET) ? explode(',', $_GET['ids']) : null,
								array_key_exists('index_above', $_GET) ? $_GET['index_above'] : null, 
								array_key_exists('index_below', $_GET) ? $_GET['index_below'] : null,
								array_key_exists('depth', $_GET) ? $_GET['depth'] : null
								);
				}
				catch(Exception $e)
				{
					report_problem($e->getMessage(), $e->getCode());
				}		
			}
		}
	}
	else if ($_SERVER['REQUEST_METHOD'] == 'PUT') #add a single record to the server
	{
		$putdata = fopen("php://input", "r");
		$json = '';
		while ($data = fread($putdata,2048)) {$json .= $data;};
		
		$wbo = new wbo();
		if (!$wbo->extract_json($json))
			report_problem(6, 400);

		if (defined('WEAVE_DATA_PROFILE'))
			error_log('PUT payload: ' . $json);
			
		
		#all server-side tests pass. now need the db connection
		try
		{
			$db = get_storage_write_object($userid, WEAVE_SHARE_DBH ? $authdb->get_connection() : null);	
		}
		catch(Exception $e)
		{
			header("X-Weave-Backoff: 1800");
			report_problem($e->getMessage(), $e->getCode());
		}

		$collection_store = new WeaveCollectionTimestamps($userid, $db);

		if (array_key_exists('HTTP_X_IF_UNMODIFIED_SINCE', $_SERVER) 
				&& $collection_store->get_max_timestamp($collection) > $_SERVER['HTTP_X_IF_UNMODIFIED_SINCE'] * 100)
			report_problem(4, 412);	
		
		#use the url if the json object doesn't have an id
		if (!$wbo->id() && $id) { $wbo->id($id); }
		
		$wbo->collection($collection);
		$wbo->modified($storage_time); #current microtime
		
		if ($wbo->validate())
		{
			try
			{
				#if there's no payload (as opposed to blank), then update the metadata
				if ($wbo->payload_exists())
				{
					$wbos = array($wbo);
					$db->store_object($wbos);
				}
				else
					$db->update_object($wbo);
			}
			catch (Exception $e)
			{
				report_problem($e->getMessage(), $e->getCode());
			}
			echo json_encode($server_time);
		}
		else
		{
			report_problem(8, 400);
		}
		
		$collection_store->memc_update($collection, $storage_time);
				
	}
	else if ($_SERVER['REQUEST_METHOD'] == 'POST')
	{
		#stupid php being helpful with input data...
		$putdata = fopen("php://input", "r");
		$jsonstring = '';
		while ($data = fread($putdata,2048)) {$jsonstring .= $data;}

		$json = json_decode($jsonstring, true);

		if ($json === null)
			report_problem(6, 400);

		if (defined('WEAVE_DATA_PROFILE'))
			error_log('POST payload: ' . $jsonstring);

		#now need the db connection
		try
		{
			$db = get_storage_write_object($userid, WEAVE_SHARE_DBH ? $authdb->get_connection() : null);	
		}
		catch(Exception $e)
		{
			header("X-Weave-Backoff: 1800");
			report_problem($e->getMessage(), $e->getCode());
		}

		$collection_store = new WeaveCollectionTimestamps($userid, $db);

		if (array_key_exists('HTTP_X_IF_UNMODIFIED_SINCE', $_SERVER) 
				&& $collection_store->get_max_timestamp($collection) > $_SERVER['HTTP_X_IF_UNMODIFIED_SINCE'] * 100)
			report_problem(4, 412);	
		
		
		$success_ids = array();
		$failed_ids = array();
		$wbos = array();
		
		
		try
		{
			$db->begin_transaction();
		}
		catch(Exception $e)
		{
			report_problem($e->getMessage(), $e->getCode());
		}

		foreach ($json as $wbo_data)
		{
			$wbo = new wbo();
			
			if (!$wbo->extract_json($wbo_data))
			{
				$failed_ids[$wbo->id()] = $wbo->get_error();
				continue;
			}
			
			$wbo->collection($collection);
			$wbo->modified($storage_time);
			
			
			if ($wbo->validate())
			{
				try
				{
					#if there's no payload (as opposed to blank), then update the metadata
					if ($wbo->payload_exists())
					{
						$wbos[] = $wbo;
					}
					else
					{
						$db->update_object($wbo);
					}
				}
				catch (Exception $e)
				{
					$failed_ids[$wbo->id()] = $e->getMessage();
					continue;
				}
				$success_ids[] = $wbo->id();
			}
			else
			{
				$failed_ids[$wbo->id()] = $wbo->get_error();
			}
		}
		
		while (count($wbos))
		{
			$wbos_slice = array_splice($wbos, 0, 100);
			try
			{
				$db->store_object($wbos_slice);
			}
			catch (Exception $e)
			{
				foreach($wbos as $wbo)
					$failed_ids[$wbo->id()] = $e->getMessage();
				continue;
			}
		}		
		
		$db->commit_transaction();

		$collection_store->memc_update($collection, $storage_time);
		
		echo json_encode(array('success' => $success_ids, 'failed' => $failed_ids));
	}
	else if ($_SERVER['REQUEST_METHOD'] == 'DELETE')
	{
		try
		{
			$db = get_storage_write_object($userid, WEAVE_SHARE_DBH ? $authdb->get_connection() : null);	
		}
		catch(Exception $e)
		{
			header("X-Weave-Backoff: 1800");
			report_problem($e->getMessage(), $e->getCode());
		}

		$collection_store = new WeaveCollectionTimestamps($userid, $db);

		if (array_key_exists('HTTP_X_IF_UNMODIFIED_SINCE', $_SERVER) 
				&& $collection_store->get_max_timestamp($collection) > $_SERVER['HTTP_X_IF_UNMODIFIED_SINCE'] * 100)
			report_problem(4, 412);	

		if ($id)
		{
			try
			{
				$db->delete_object($collection, $id);
			}
			catch(Exception $e)
			{
				report_problem($e->getMessage(), $e->getCode());
			}
			echo json_encode($server_time);
		}
		else
		{
			try
			{
				$db->delete_objects($collection, null,  
							array_key_exists('parentid', $_GET) ? $_GET['parentid'] : null, 
							array_key_exists('predecessorid', $_GET) ? $_GET['predecessorid'] : null, 
							array_key_exists('newer', $_GET) ? $_GET['newer'] * 100 : null, 
							array_key_exists('older', $_GET) ? $_GET['older'] * 100 : null, 
							array_key_exists('sort', $_GET) ? $_GET['sort'] : null, 
							array_key_exists('limit', $_GET) ? $_GET['limit'] : null, 
							array_key_exists('offset', $_GET) ? $_GET['offset'] : null,
							array_key_exists('ids', $_GET) ? explode(',', $_GET['ids']) : null,
							array_key_exists('index_above', $_GET) ? $_GET['index_above'] : null, 
							array_key_exists('index_below', $_GET) ? $_GET['index_below'] : null
							);			
			}
			catch(Exception $e)
			{
				report_problem($e->getMessage(), $e->getCode());
			}
			echo json_encode($server_time);
		}
	}
	else
	{
		#bad protocol. There are protocols left? HEAD, I guess.
		report_problem(1, 400);
	}


class WeaveCollectionTimestamps
{
	private $_memc = null;
	private $_userid = null;
	private $_collections = null;
	private $_db = null;
	
	function __construct ($userid, $db)
	{
		$this->_userid = $userid;
		$this->_db = $db;
		
		if (defined('WEAVE_STORAGE_MEMCACHE_PORT') && WEAVE_STORAGE_MEMCACHE_PORT)
		{
			$this->_memc = new Memcache;
			try
			{
				$this->_memc->connect('localhost', WEAVE_STORAGE_MEMCACHE_PORT);
			}			
			catch( Exception $exception )
			{
				error_log($exception->getMessage());
				$this->_memc = null;
			}				
		}
	}
	
	function get_max_timestamp($collection)
	{
		if ($this->_memc) #to our advantage to leverage the collection object we'll be updating soon.
		{
			$this->memc_retrieve();
			return $this->_collections[$collection];
		}
		else
		{
			return $this->_db->get_max_timestamp($collection);
		}
	}
	
	function get_collection_timestamps()
	{
		$this->memc_retrieve();
		return $this->_collections;
	}
	
	function memc_retrieve()
	{
		if ($this->_collections)
			return $this->_collections;
			
		if ($this->_memc)
		{
			if ($collections = $this->_memc->get('coll:' . $this->_userid))
			{
				$this->_collections = $collections;
				return;
			}
		}	
		
		$collections = $this->_db->get_collection_list_with_timestamps();
		
		$this->_collections = $collections;
		$this->memc_set();
	}
	
	function memc_update($collection, $timestamp)
	{
		if ($this->_memc)
		{
			$this->memc_retrieve();
			$this->_collections[$collection] = $timestamp;
			$this->memc_set();
		}	
	}
	
	function memc_set()
	{
		if ($this->_memc && $this->_collections)
			$this->_memc->set('coll:' . $this->_userid, $this->_collections, false, WEAVE_STORAGE_MEMCACHE_DECAY);	
	}
}

#The datasets we might be dealing with here are too large for sticking it all into an array, so
#we need to define a direct-output method for the storage class to use. If we start producing multiples
#(unlikely), we can put them in their own class.

class WBOJsonOutput  
{
	private $_full = null;
	private $_comma_flag = 0;
	private $_output_format = 'json';
	
	function __construct ($full = null)
	{
		$this->_full = $full;
	}

	function set_format($format)
	{
		$this->_output_format = $format;
	}
	
	
	function output($sth)
	{
		if (($rowcount = $sth->rowCount()) > 0)
		{
			header('X-Weave-Records: ' . $rowcount);
		}
		if ($this->_output_format == 'newlines')
		{
			return $this->output_newlines($sth);
		}
		elseif ($this->_output_format == 'whoisi')
		{
			return $this->output_whoisi($sth);
		}
		else
		{
			return $this->output_json($sth);
		}
	}
	
	function output_json($sth)
	{
		echo '[';
		
		while ($result = $sth->fetch(PDO::FETCH_ASSOC))
		{
			if ($this->_comma_flag) { echo ','; } else { $this->_comma_flag = 1; }
			if ($this->_full)
			{
				$wbo = new wbo();
				$wbo->populate($result);
				echo $wbo->json();
			}
			else
				echo json_encode($result{'id'});
		}

		echo ']';
		return 1;
	}

	function output_whoisi($sth)
	{		
		while ($result = $sth->fetch(PDO::FETCH_ASSOC))
		{
			if ($this->_full)
			{
				$wbo = new wbo();
				$wbo->populate($result);
				$output = $wbo->json();
			}
			else
				$output = json_encode($result{'id'});
			echo pack('N', mb_strlen($output, '8bit')) . $output;
		}
		return 1;
	}

	function output_newlines($sth)
	{		
		while ($result = $sth->fetch(PDO::FETCH_ASSOC))
		{
			if ($this->_full)
			{
				$wbo = new wbo();
				$wbo->populate($result);
				echo preg_replace('/\n/', '\u000a', $wbo->json());
			}
			else
				echo json_encode($result{'id'});
			echo "\n";
		}
		return 1;
	}
}
?>
