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
	require_once 'weave_storage/' . WEAVE_STORAGE_ENGINE . '.php';
	require_once 'weave_basic_object.php';
	require_once 'weave_metadata.php';
	require_once 'weave_utils.php';
	
	header("Content-type: application/json");
	
	$server_time = round(microtime(1), 2);
	header("X-Weave-Timestamp: " . $server_time);
	$storage_time = $server_time * 100; #internal representation as bigint

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
		$db = new WeaveStorage($userid);	
		
		$metadata_store = new WeaveMetadata($userid, $db);

		if ($_SERVER['REQUEST_METHOD'] == 'GET')
		{
			if ($function == 'info')
			{
				switch ($collection)
				{
					case 'quota':
						exit(json_encode(array((int)($metadata_store->storage_total_get()/1024), $metadata_store->get_system_quota_display())));
					case 'collections':
						$results = $metadata_store->get_collection_timestamps();
						foreach ($results as $k => $v)
							$results[$k] = $results[$k]/100;
						exit(json_encode($results));
					case 'collection_counts':
						exit(json_encode($metadata_store->get_collection_list_with_counts()));
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
						echo $wbo[0]->json();
					else
						report_problem("record not found", 404);
				}
				else #retrieve a batch of records. Sadly, due to potential record sizes, have the storage object stream the output...
				{
					$full = array_key_exists('full', $_GET) && $_GET['full'];

					require_once 'weave_output.php';
					$outputter = new WBOOutput($full);
		
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
			}
		}
		else if ($_SERVER['REQUEST_METHOD'] == 'PUT') #add a single record to the server
		{		
			$wbo = new wbo();
			if (!$wbo->extract_json(get_json()))
				report_problem(WEAVE_ERROR_JSON_PARSE, 400);
							
			$metadata_store->check_quota();
			$metadata_store->check_timestamp($collection);
			
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
					$metadata_store->storage_total_add(strlen($wbo->payload()));
				}
				else
					$db->update_object($wbo);
			}
			else
			{
				report_problem(WEAVE_ERROR_INVALID_WBO, 400);
			}
			
			$metadata_store->collections_update($collection, $storage_time);

			echo json_encode($server_time);
					
		}
		else if ($_SERVER['REQUEST_METHOD'] == 'POST')
		{
			
			$json = get_json();
			
			$metadata_store->check_quota();
			$metadata_store->check_timestamp($collection);
			
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
						$db->update_object($wbo);
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
				}
				catch (Exception $e)
				{
					foreach($wbos as $wbo)
						$failed_ids[$wbo->id()] = $e->getMessage();
					continue;
				}
			}		
			
			$db->commit_transaction();
	
			$metadata_store->storage_total_add($payload_size_total);
			$metadata_store->collections_update($collection, $storage_time);
			
			echo json_encode(array('success' => $success_ids, 'failed' => $failed_ids));
		}
		else if ($_SERVER['REQUEST_METHOD'] == 'DELETE')
		{	
			$metadata_store->check_timestamp($collection);
			
			if ($id)
			{
				$db->delete_object($collection, $id);
			}
			else if ($collection)
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
			else
			{
				if (!array_key_exists('HTTP_X_CONFIRM_DELETE', $_SERVER))
					report_problem(WEAVE_ERROR_NO_OVERWRITE, 412);
				$db->delete_user();
			}
	
			$metadata_store->collections_update($collection, $storage_time);
			$metadata_store->storage_total_flush(); #don't know the impact, so simply delete it and grab it next time
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
