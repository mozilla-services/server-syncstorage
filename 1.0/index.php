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
# Portions created by the Initial Developer are Copyright (C) 2010
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
	require_once 'weave_basic_object.php';
	require_once 'weave_utils.php';
	require_once 'cef.php';

	$cef = new CommonEventFormat('mozilla', WEAVE_CEF_DEVICE, '1.3');
	
	header("Content-type: application/json");
	
	$server_time = round(microtime(1), 2);
	header("X-Weave-Timestamp: " . $server_time);
	$storage_time = round($server_time * 100); #internal representation as bigint

	#Basic path extraction and validation. No point in going on if these are missing
	$path = '/';
	if (!empty($_SERVER['PATH_INFO'])) 
		$path = $_SERVER['PATH_INFO'];
	else if (!empty($_SERVER['ORIG_PATH_INFO']))
		$path = $_SERVER['ORIG_PATH_INFO'];
	else
		report_problem("No path found", 404);
	
	$path = substr($path, 1); #chop the lead slash
	list($username, $function, $collection, $id) = explode('/', $path . '///');
	
	if ($function != "info" && $function != "storage")
		report_problem(WEAVE_ERROR_FUNCTION_NOT_SUPPORTED, 400);
	
	if (!validate_username($username))
		report_problem(WEAVE_ERROR_INVALID_USERNAME, 400);
	
	#only a delete has meaning without a collection
	if ($collection)
	{
		if (!validate_collection($collection))
			report_problem(WEAVE_ERROR_INVALID_COLLECTION, 400);		
	}
	else if ($_SERVER['REQUEST_METHOD'] != 'DELETE')
		report_problem(WEAVE_ERROR_INVALID_PROTOCOL, 400);

	
	#Auth the user
	$userid = verify_user($username);
		
	#quick check to make sure that any non-storage function calls are just using GET
	if ($function != 'storage' && $_SERVER['REQUEST_METHOD'] != 'GET')
		report_problem(WEAVE_ERROR_INVALID_PROTOCOL, 400);
	


	#user passes preliminaries, connections made, onto actually getting the data
	try
	{
		$db = null;
		$db2 = null;
		
		if (defined('WEAVE_STORAGE_MEMCACHE_HOST') && WEAVE_STORAGE_MEMCACHE_HOST)
		{
			require_once 'weave_storage/memcache_layer.php';
			$db = new WeaveMemcache($userid);	
		}
		else
		{
			require_once 'weave_storage/' . WEAVE_STORAGE_ENGINE . '.php';
			$db = new WeaveStorage($userid);	
		}	
		
		if (defined('WEAVE_STORAGE_SECONDARY_ENGINE') && WEAVE_STORAGE_SECONDARY_ENGINE)
		{
			require_once 'weave_secondary_storage/' . WEAVE_STORAGE_SECONDARY_ENGINE . '.php';
			$db2 = new WeaveSecondaryStorage($userid);
		}
		
		if ($_SERVER['REQUEST_METHOD'] == 'GET')
		{
			if ($function == 'info')
			{
				switch ($collection)
				{
					case 'quota':
						exit(json_encode(array((int)($db->get_storage_total()/1024), defined('WEAVE_QUOTA') ? (int)(WEAVE_QUOTA/1024) : null)));
					case 'collections':

						$results = $db->get_collection_list_with_timestamps();
						foreach ($results as $k => $v)
							$results[$k] = $results[$k]/100;
						exit(json_encode($results));
					case 'collection_counts':
						exit(json_encode($db->get_collection_list_with_counts()));
					default:
						report_problem(WEAVE_ERROR_INVALID_PROTOCOL, 400);
				}
			}
			elseif ($function == 'storage')
			{
				if ($id) #retrieve a single record
				{
					$wbo = $db->retrieve_objects($collection, $id, 1); #get the full contents of one record
					if (count($wbo) > 0)
					{
						$item = array_shift($wbo);
						echo $item->json();
					}
					else
						report_problem("record not found", 404);
				}
				else #retrieve a batch of records. Sadly, due to potential record sizes, have the storage object stream the output...
				{
					$full = array_key_exists('full', $_GET) && $_GET['full'];

					require_once 'weave_output.php';
					$outputter = new WBOOutput($full);
		
					$params = validate_search_params();

					$ids = $db->retrieve_objects($collection, null, $full, $outputter,
								$params['parentid'], $params['predecessorid'], 
								$params['newer'], $params['older'], 
								$params['sort'], 
								$params['limit'], $params['offset'], 
								$params['ids'], 
								$params['index_above'], $params['index_below'], $params['depth']
								);
				}
			}
		}
		else if ($_SERVER['REQUEST_METHOD'] == 'PUT') #add a single record to the server
		{		
			$wbo = new wbo();
			if (!$wbo->extract_json(get_json()))
				report_problem(WEAVE_ERROR_JSON_PARSE, 400);
							
			check_quota($db);
			check_timestamp($collection, $db);
			
			#use the url if the json object doesn't have an id
			if (!$wbo->id() && $id) { $wbo->id($id); }
			
			$wbo->collection($collection);
			$wbo->modified($storage_time); #current microtime
			
			if ($wbo->validate())
			{
				#if there's no payload (as opposed to blank), then update the object
				if ($wbo->payload_exists())
				{
					$wbos = array($wbo);
					$db->store_object($wbos);
					if ($db2)
					{
						try
						{
							$db2->store_object($wbos);
						}
						catch(Exception $e)
						{
							#ignore any error. Backend should have logged it
						}					
					}
				}
				else
					$db->update_object($wbo);
					if ($db2)
					{
						try
						{
							$db2->update_object($wbo);
						}
						catch(Exception $e)
						{
							#ignore any error. Backend should have logged it
						}					
					}
			}
			else
			{
				report_problem(WEAVE_ERROR_INVALID_WBO, 400);
			}
			

			echo json_encode($server_time);
					
		}
		else if ($_SERVER['REQUEST_METHOD'] == 'POST')
		{		
			$json = get_json();
			
			check_quota($db);
			check_timestamp($collection, $db);
			
			$payload_size_total = 0;
			$success_ids = array();
			$failed_ids = array();
			$wbos = array();
						
			$db->begin_transaction();
	
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
					#if there's no payload (as opposed to blank), then update the metadata
					if ($wbo->payload_exists())
						$wbos[] = $wbo;
					else
					{
						$db->update_object($wbo);
						if ($db2)
						{
							try
							{
								$db2->update_object($wbo);
							}
							catch(Exception $e)
							{
								#ignore any error. Backend should have logged it
							}					
						}
					}						
					$payload_size_total += strlen($wbo->payload());
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
					if ($db2)
					{
						try
						{
							$db2->store_object($wbos_slice);
						}
						catch(Exception $e)
						{
							#ignore any error. Backend should have logged it
						}					
					}
				}
				catch (Exception $e)
				{
					foreach($wbos as $wbo)
						$failed_ids[$wbo->id()] = $e->getMessage();
					continue;
				}
			}		
			
			$db->commit_transaction();
	
			echo json_encode(array('success' => $success_ids, 'failed' => $failed_ids));
		}
		else if ($_SERVER['REQUEST_METHOD'] == 'DELETE')
		{	
			check_timestamp($collection, $db);
			
			if ($id)
			{
				$db->delete_object($collection, $id);
				if ($db2)
				{
					try
					{
						$db2->delete_object($collection, $id);
					}
					catch(Exception $e)
					{
						#ignore any error. Backend should have logged it
					}					
				}
			}
			else if ($collection)
			{
				$params = validate_search_params();

				$db->delete_objects($collection, null,  
								$params['parentid'], $params['predecessorid'], 
								$params['newer'], $params['older'], 
								$params['sort'], 
								$params['limit'], $params['offset'], 
								$params['ids'], 
								$params['index_above'], $params['index_below']
							);			
				if ($db2)
				{
					try
					{
						$db->delete_objects($collection, null,  
									$params['parentid'], $params['predecessorid'], 
									$params['newer'], $params['older'], 
									$params['sort'], 
									$params['limit'], $params['offset'], 
									$params['ids'], 
									$params['index_above'], $params['index_below']
							);			
					}
					catch(Exception $e)
					{
						#ignore any error. Backend should have logged it
					}					
				}
			}
			else
			{
				if (!array_key_exists('HTTP_X_CONFIRM_DELETE', $_SERVER))
					report_problem(WEAVE_ERROR_NO_OVERWRITE, 412);
				$db->delete_user();
				if ($db2)
				{
					try
					{
						$db2->delete_user();
					}
					catch(Exception $e)
					{
						#ignore any error. Backend should have logged it
					}					
				}
			}
	
			echo json_encode($server_time);

		}
		else
		{
			#bad protocol. There are protocols left? HEAD, I guess.
			report_problem(WEAVE_ERROR_INVALID_PROTOCOL, 400);
		}
	}
	catch(Exception $e)
	{
		report_problem($e->getMessage(), $e->getCode());
	}

?>
