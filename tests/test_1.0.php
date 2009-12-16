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

	$protocol = "https";
	$host = "localhost";
	$server = "$protocol://$host";
	$prefix = "";
	$version = "1.0";
	$username = null;
	$password = null;
	
	if (file_exists('../' . $version . '/' . $host . '_constants.php'))
		require_once '../' . $version . '/' . $host . '_constants.php';

	if (file_exists('../' . $version . '/default_constants.php'))
		require_once '../' . $version . '/default_constants.php';
	
	if (defined('WEAVE_DEFAULT_TEST_USERNAME'))
		$username = WEAVE_DEFAULT_TEST_USERNAME;
	
	if (defined('WEAVE_DEFAULT_TEST_PASSWORD'))
		$password = WEAVE_DEFAULT_TEST_PASSWORD;
		
	if (!$username)
	{
		echo "Please enter a username: ";
		$handle = fopen ("php://stdin","r");
		$username = trim(fgets($handle));
	}
	
	if (!$password)
	{
		echo "Please enter a password: ";
		$handle = fopen ("php://stdin","r");
		$password = trim(fgets($handle));
	}
	
	$item1 = '{"id": 1, "sortindex": 1, "payload": "123456789abcdef"}';
	$item2 = '{"id": 2, "sortindex": 2, "payload":"abcdef123456789"}';
	$item3 = '{"id": 3, "parentid": 1, "sortindex": 3, "payload":"123abcdef123456789"}';
	$item4 = '{"id": 4, "parentid": 1, "sortindex": 4, "payload":"567abcdef123456789"}';
	$item5 = '{"parentid": 1, "sortindex": 4, "payload":"567abcdef123456789"}';
	$item6 = '{"id": 4, "parentid": 1, "sortindex": 5}';
	
	
	$timestamp1 = put_item('history', $item1);
	output_test("Put an item", is_numeric($timestamp1) && $timestamp1 > 1000000000);
	
	if (defined('WEAVE_QUOTA'))
		output_test("Check quota", get_quota($username) == ('[0,' . (int)(WEAVE_QUOTA/1024) . ']'));

	$timestamp2 = put_item('foo', $item2);
	output_test("Put an item", is_numeric($timestamp2) && $timestamp2 > 1000000000);

	output_test("Get collection counts", compare_arrays(json_get(get_collection_counts()), array('history' => "1", 'foo' => "1")));

	output_test("Get collection timestamps", compare_arrays(json_get(get_collection_timestamps()), array('history' => $timestamp1, 'foo' => $timestamp2)));

	output_test("Get item", compare_arrays(json_get(get_item('foo', '2')), array('id' => "2", 'modified' => $timestamp2, 'sortindex' => "2", 'payload' => 'abcdef123456789')));

	output_test("Get collection ids", get_collection_ids('foo') == '["2"]');

	output_test("Bad put (timestamp too old)", put_item('foo', $item1, $timestamp2 - 1) == 4);
	
	output_test("Post", post_items('foo', '[' . $item4 . ',' . $item3 . ',' . $item5 . ']') == '{"success":[4,3],"failed":{"":["invalid id"]}}');

	output_test("Get collection ids (sortindex)", get_collection_ids('foo', "sort=index") == '["4","3","2"]');

	output_test("Get items by parent id", get_collection_ids('foo', 'sort=index&parentid=1') == '["4","3"]');

	$timestamp3 = delete_item('foo', "3");
	output_test("Delete item", is_numeric($timestamp1) && $timestamp1 > 1000000000);

	output_test("Get collection ids (sortindex)", get_collection_ids('foo', "sort=index") == '["4","2"]');

	output_test("Get collection counts", compare_arrays(json_get(get_collection_counts()), array('history' => "1", 'foo' => "2")));

	$timestamp4 = put_item('foo', $item6); #updates item 4
	output_test("Update item", is_numeric($timestamp4) && $timestamp4 > 1000000000);

	output_test("Get item", compare_arrays(json_get(get_item('foo', '4')), array('id' => "4", 'parentid' => '1', 'modified' => $timestamp4, 'sortindex' => "5", 'payload' => '567abcdef123456789')));

	$timestamp5 = delete_items_by_timestamp('foo', $timestamp2 + .01);
	output_test("Delete items by timestamp", is_numeric($timestamp5) && $timestamp5 > 1000000000);

	output_test("Get collection counts", compare_arrays(json_get(get_collection_counts()), array('history' => "1", 'foo' => "1")));

	output_test("Get collection timestamps", compare_arrays(json_get(get_collection_timestamps()), array('history' => $timestamp1, 'foo' => $timestamp4)));

	output_test("Delete all records (no confirmation)", delete_all_no_confirm() == 4);
	
	$timestamp = delete_all();
	output_test("Delete all records", is_numeric($timestamp) && $timestamp > 1000000000);
	
	
	
	function output_test ($name, $condition)
	{
		echo $name;
		echo '...';
		echo $condition ? 'OK' : 'NOT OK';
		echo "\n";
		return $condition;
	}

	function json_get($string)
	{
		$tmp = json_decode($string, true);
		foreach ($tmp as $k => $v)
			$tmp[$k] = (string)$v;
		
		return $tmp;
	}
	
	function compare_arrays($a1, $a2)
	{
		if (!is_array($a1))
			return false;
		if (!is_array($a2))
			return false;

		foreach ($a1 as $k => $v)
		{
			if ($a2[$k] != $v)
				return false;
		}
		foreach ($a2 as $k => $v)
		{
			if ($a1[$k] != $v)
				return false;
		}
		return true;
	}
	
	
	function get_quota()
	{
		global $prefix, $version, $username, $password;
		$result = curl_get("/$prefix/$version/$username/info/quota", $username, $password);
		return $result;
	}
	
	function get_collection_counts()
	{
		global $prefix, $version, $username, $password;
		$result = curl_get("/$prefix/$version/$username/info/collection_counts", $username, $password);
		return $result;
	}
	
	function get_collection_timestamps()
	{
		global $prefix, $version, $username, $password;
		$result = curl_get("/$prefix/$version/$username/info/collections", $username, $password);
		return $result;
	}
	
	function get_collection_ids($collection, $params = null)
	{
		global $prefix, $version, $username, $password;
		$result = curl_get("/$prefix/$version/$username/storage/$collection?$params", $username, $password);
		return $result;
	}
	
	function get_item($collection, $id)
	{
		global $prefix, $version, $username, $password;
		$result = curl_get("/$prefix/$version/$username/storage/$collection/$id?full=1", $username, $password);
		return $result;
	}
	
	function delete_item($collection, $id)
	{
		global $prefix, $version, $username, $password;
		$result = curl_delete("/$prefix/$version/$username/storage/$collection/$id", $username, $password);
		return $result;
	}
	
	function delete_items_by_timestamp($collection, $timestamp)
	{
		global $prefix, $version, $username, $password;
		$result = curl_delete("/$prefix/$version/$username/storage/$collection?older=$timestamp", $username, $password);
		return $result;
	}
	
	function delete_all()
	{
		global $prefix, $version, $username, $password;
		$result = curl_delete("/$prefix/$version/$username/storage", $username, $password, array('X-Confirm-Delete: true'));
		return $result;
	}
	
	function delete_all_no_confirm()
	{
		global $prefix, $version, $username, $password;
		$result = curl_delete("/$prefix/$version/$username/storage", $username, $password);
		return $result;
	}
	
	function put_item($collection, $payload, $modification_date = null)
	{
		global $prefix, $version, $username, $password;
		$result = curl_put("/$prefix/$version/$username/storage/$collection", $payload, $username, $password, $modification_date);
		return $result;
	}
	
	function post_items($collection, $payload, $modification_date = null)
	{
		global $prefix, $version, $username, $password;
		$result = curl_post("/$prefix/$version/$username/storage/$collection", $payload, $username, $password, $modification_date);
		return $result;
	}

	
	
	function curl_get($url, $username, $password)
	{
		global $server; 
		$ch = curl_init($server . '/' . $url);
		if ($password)
			curl_setopt($ch, CURLOPT_USERPWD, $username . ":" . $password);
		curl_setopt($ch, CURLOPT_RETURNTRANSFER, true);
		curl_setopt($ch, CURLOPT_SSL_VERIFYHOST, false);
		$result = curl_exec($ch);
		curl_close($ch);
		
		return $result;
	}
	
	function curl_post($url, $body, $username, $password = null, $mod_date)
	{
		global $server; 
		$ch = curl_init($server . '/' . $url);
		if ($password)
			curl_setopt($ch, CURLOPT_USERPWD, $username . ":" . $password);
		if ($mod_date)
			curl_setopt($ch, CURLOPT_HTTPHEADER, array("X-If-Unmodified-Since: $mod_date"));
		curl_setopt($ch, CURLOPT_SSL_VERIFYHOST, false);
		curl_setopt($ch, CURLOPT_RETURNTRANSFER, true);
		curl_setopt($ch, CURLOPT_POST, true);
		curl_setopt($ch, CURLOPT_POSTFIELDS, $body);
		$result = curl_exec($ch);
		curl_close($ch);
		
		return $result;
	}
	
	function curl_put($url, $body, $username, $password, $mod_date)
	{
		global $server; 
		$data = tmpfile();
		fwrite($data, $body);
		fseek($data, 0);
		
		$ch = curl_init($server . '/' . $url);
		if ($username && $password)
			curl_setopt($ch, CURLOPT_USERPWD, $username . ":" . $password);
		if ($mod_date)
			curl_setopt($ch, CURLOPT_HTTPHEADER, array("X-If-Unmodified-Since: $mod_date"));
		curl_setopt($ch, CURLOPT_SSL_VERIFYHOST, false);
		curl_setopt($ch, CURLOPT_RETURNTRANSFER, true);
		curl_setopt($ch, CURLOPT_PUT, true);
		curl_setopt($ch, CURLOPT_INFILE, $data);
		curl_setopt($ch, CURLOPT_INFILESIZE, strlen($body));
		$result = curl_exec($ch);
		fclose($data);
		curl_close($ch);
		
		return $result;
	}
	
	function curl_delete($url, $username, $password, $header = null)
	{
		global $server; 
		$ch = curl_init($server . '/' . $url);
		if ($username && $password)
			curl_setopt($ch, CURLOPT_USERPWD, $username . ":" . $password);
		if ($header)
			curl_setopt($ch, CURLOPT_HTTPHEADER, $header);
		curl_setopt($ch, CURLOPT_SSL_VERIFYHOST, false);
		curl_setopt($ch, CURLOPT_RETURNTRANSFER, true);
		curl_setopt($ch, CURLOPT_CUSTOMREQUEST, 'DELETE');
		$result = curl_exec($ch);
		curl_close($ch);
		
		return $result;
	}
	
	


?>