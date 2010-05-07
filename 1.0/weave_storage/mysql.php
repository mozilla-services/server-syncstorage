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
	
require_once 'weave_storage/base.php';
require_once 'weave_basic_object.php';


#Mysql version of the storage object.
#Note that this object does not contain any database setup information. It assumes that the mysql
#instance is already fully configured

#CREATE TABLE `collections` (
#  `userid` int(11) NOT NULL,
#  `collectionid` smallint(6) NOT NULL,
#  `name` varchar(32) NOT NULL,
#  PRIMARY KEY  (`userid`,`collectionid`),
#  KEY `nameindex` (`userid`,`name`)
#) ENGINE=InnoDB;
#
#CREATE TABLE `wbo` (
#  `username` int(11) NOT NULL,
#  `collection` smallint(6) NOT NULL default '0',
#  `id` varbinary(64) NOT NULL default '',
#  `parentid` varbinary(64) default NULL,
#  `predecessorid` varbinary(64) default NULL,
#  `sortindex` int(11) default NULL,
#  `modified` bigint(20) default NULL,
#  `payload` longtext,
#  `payload_size` int(11) default NULL,
#  PRIMARY KEY  (`username`,`collection`,`id`),
#  KEY `parentindex` (`username`,`collection`,`parentid`),
#  KEY `modified` (`username`,`collection`,`modified`),
#  KEY `weightindex` (`username`,`collection`,`sortindex`),
#  KEY `predecessorindex` (`username`,`collection`,`predecessorid`),
#  KEY `size_index` (`username`,`payload_size`)
#) ENGINE=InnoDB;

class WeaveStorage implements WeaveStorageBase
{
	private $_username;
	private $_dbh = null;
	private $_db_name = 'wbo';
	private $_collection_table_name = 'collections';

	private $WEAVE_COLLECTION_KEYS = array('clients' => 1, 'crypto' => 2, 'forms' => 3, 'history' => 4,
									'keys' => 5, 'meta' => 6, 'bookmarks' => 7, 'prefs' => 8, 'tabs' => 9,
									'passwords' => 10);
									
	private $WEAVE_COLLECTION_NAMES;
	
	function __construct($username) 
	{
		$this->_username = $username;
		$this->WEAVE_COLLECTION_NAMES = array_flip($this->WEAVE_COLLECTION_KEYS);
		

		if (defined('WEAVE_MYSQL_STORE_TABLE_NAME'))
			$this->_db_name = WEAVE_MYSQL_STORE_TABLE_NAME;
		
		if (defined('WEAVE_USER_TABLE_MODULO') && is_int(WEAVE_USER_TABLE_MODULO))
			$this->_db_name .= ($this->_username % WEAVE_USER_TABLE_MODULO);
			
		if (defined('WEAVE_MYSQL_COLLECTION_TABLE_NAME'))
			$this->_collection_table_name = WEAVE_MYSQL_COLLECTION_TABLE_NAME;
	}

	function open_connection() 
	{		
		if ($this->_dbh)
			return;
			
		try
		{
			if ($_SERVER['REQUEST_METHOD'] == 'GET')
			{
				$this->_dbh = new PDO('mysql:host=' . WEAVE_MYSQL_STORE_READ_HOST . ';dbname=' . WEAVE_MYSQL_STORE_READ_DB, 
									WEAVE_MYSQL_STORE_READ_USER, WEAVE_MYSQL_STORE_READ_PASS); 
			}
			else
			{
				$this->_dbh = new PDO('mysql:host=' . WEAVE_MYSQL_STORE_WRITE_HOST . ';dbname=' . WEAVE_MYSQL_STORE_WRITE_DB, 
									WEAVE_MYSQL_STORE_WRITE_USER, WEAVE_MYSQL_STORE_WRITE_PASS);
			}
			$this->_dbh->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
		}
		catch( PDOException $exception )
		{
			error_log($exception->getMessage());
			throw new Exception("Database unavailable", 503);
		}
	}
	
	function get_connection()
	{
		$this->open_connection();
		return $this->_dbh;
	}

	function begin_transaction()
	{
		$this->open_connection();
		try
		{
			$this->_dbh->beginTransaction();
		}
		catch( PDOException $exception )
		{
			error_log("begin_transaction: " . $exception->getMessage());
			throw new Exception("Database unavailable", 503);
		}
		return true;
	}

	function commit_transaction()
	{
		$this->_dbh->commit();
		return true;
	}
	
	function get_collection_id($collection)
	{
		$this->open_connection();
		if (!$collection)
		{
			return null;
		}
		
		if (array_key_exists($collection, $this->WEAVE_COLLECTION_KEYS))
			return $this->WEAVE_COLLECTION_KEYS[$collection];
		
		try
		{
			$select_stmt = 'select collectionid from ' . $this->_collection_table_name . ' where userid = :userid and name = :collection';
				
			$sth = $this->_dbh->prepare($select_stmt);
			$sth->bindParam(':userid', $this->_username);
			$sth->bindParam(':collection', $collection);
			$sth->execute();
		}
		catch( PDOException $exception )
		{
			error_log("get_collection_id: " . $exception->getMessage());
			throw new Exception("Database unavailable", 503);
		}
		
		$result = $sth->fetchColumn();
		if (!$result)
		{
			$result = $this->store_collection_id($collection);
		}
		
		return $result;		
		
	}

	function store_collection_id($collection)
	{
		$this->open_connection();

		#get the current max collection id
		try
		{
			$select_stmt = 'select max(collectionid) from ' . $this->_collection_table_name . ' where userid = :userid';
				
			$sth = $this->_dbh->prepare($select_stmt);
			$sth->bindParam(':userid', $this->_username);
			$sth->execute();
		}
		catch( PDOException $exception )
		{
			error_log("store_collection_id: " . $exception->getMessage());
			throw new Exception("Database unavailable", 503);
		}
		$result = $sth->fetchColumn();
		if (!$result)
			$result = 100;
		$result += 1;
		
		$sth->closeCursor();

		$insert_stmt = 'insert into ' . $this->_collection_table_name . ' (userid, collectionid, name) values (?, ?, ?)';
		$values = array($this->_username, $result, $collection);
		
		try
		{
			$sth = $this->_dbh->prepare($insert_stmt);
			$sth->execute($values);
		}
		catch( PDOException $exception )
		{
			error_log("store_collection_id: " . $exception->getMessage());
			throw new Exception("Database unavailable", 503);
		}
		
		return $result;
		
	}

	function get_collection_name($collection_id)
	{		
		$this->open_connection();

		if (!$collection_id)
		{
			return null;
		}
		
		if (array_key_exists($collection, $this->WEAVE_COLLECTION_NAMES))
			return $this->WEAVE_COLLECTION_NAMES[$collection_id];
		
		try
		{
			$select_stmt = 'select name from ' . $this->_collection_table_name . ' where userid = :userid and collectionid = :collection';
				
			$sth = $this->_dbh->prepare($select_stmt);
			$sth->bindParam(':userid', $this->_username);
			$sth->bindParam(':collection', $collection_id);
			$sth->execute();
		}
		catch( PDOException $exception )
		{
			error_log("get_collection_name: " . $exception->getMessage());
			throw new Exception("Database unavailable", 503);
		}
		
		$result = $sth->fetchColumn();
		return $result;		
		
	}

	function get_users_collection_list()
	{
		$this->open_connection();

		try
		{
			$select_stmt = 'select collectionid, name from ' . $this->_collection_table_name . ' where userid = :userid';
			$sth = $this->_dbh->prepare($select_stmt);
			$sth->bindParam(':userid', $this->_username);
			$sth->execute();
		}
		catch( PDOException $exception )
		{
			error_log("get_users_collection_list: " . $exception->getMessage());
			throw new Exception("Database unavailable", 503);
		}
		
		
		$collections = array();
		while ($result = $sth->fetch(PDO::FETCH_NUM))
		{
			$this->WEAVE_COLLECTION_NAMES[$result[0]] = $result[1];
		}
		
		return $this->WEAVE_COLLECTION_NAMES;		
		
	}
	
	
	function get_max_timestamp($collection)
	{
		$this->open_connection();

		if (!$collection)
		{
			return null;
		}
		
		try
		{
			$select_stmt = 'select max(modified) from ' . $this->_db_name . ' where username = :username and collection = :collection';
			$collection = $this->get_collection_id($collection);
				
			$sth = $this->_dbh->prepare($select_stmt);
			$sth->bindParam(':username', $this->_username);
			$sth->bindParam(':collection', $collection);
			$sth->execute();
		}
		catch( PDOException $exception )
		{
			error_log("get_max_timestamp: " . $exception->getMessage());
			throw new Exception("Database unavailable", 503);
		}
		
		$result = $sth->fetchColumn();
		return $result;		
	}

	function get_collection_list()
	{		
		$this->open_connection();

		try
		{
			$select_stmt = 'select distinct(collection) from ' . $this->_db_name . ' where username = :username';
			$sth = $this->_dbh->prepare($select_stmt);
			$sth->bindParam(':username', $this->_username);
			$sth->execute();
		}
		catch( PDOException $exception )
		{
			error_log("get_collection_list: " . $exception->getMessage());
			throw new Exception("Database unavailable", 503);
		}
		$results = $sth->fetchAll(PDO::FETCH_NUM);
		$sth->closeCursor();
			
		$collections = array();
		foreach ($results as $result)
		{
			if (!array_key_exists($result, $this->WEAVE_COLLECTION_NAMES) && !$user_collections)
			{
				$this->get_users_collection_list();
				$user_collections = 1;
			}
			
			if (array_key_exists($result[0], $this->WEAVE_COLLECTION_NAMES))
				$result = $this->WEAVE_COLLECTION_NAMES[$result];
			else
				continue;

			$collections[] = $result;
		}
		
		return $collections;		
	}

	function get_collection_list_with_timestamps()
	{
		$this->open_connection();

		try
		{
			$select_stmt = 'select collection, max(modified) as timestamp from ' . $this->_db_name . ' where username = :username group by collection';
			$sth = $this->_dbh->prepare($select_stmt);
			$sth->bindParam(':username', $this->_username);
			$sth->execute();
		}
		catch( PDOException $exception )
		{
			error_log("get_collection_list: " . $exception->getMessage());
			throw new Exception("Database unavailable", 503);
		}
		$results = $sth->fetchAll(PDO::FETCH_NUM);
		$sth->closeCursor();
		
		$collections = array();
		$user_collections = 0;
		foreach ($results as $result)
		{
			if (!array_key_exists($result[0], $this->WEAVE_COLLECTION_NAMES) && !$user_collections)
			{
				$this->get_users_collection_list();
				$user_collections = 1;
			}
			
			if (array_key_exists($result[0], $this->WEAVE_COLLECTION_NAMES))
				$result[0] = $this->WEAVE_COLLECTION_NAMES[$result[0]];
			else
				continue;

			$collections[$result[0]] = $result[1];
		}
		return $collections;		
	}
	
	function get_collection_list_with_counts()
	{
		$this->open_connection();

		try
		{
			$select_stmt = 'select collection, count(*) as ct from ' . $this->_db_name . ' where username = :username group by collection';
			$sth = $this->_dbh->prepare($select_stmt);
			$sth->bindParam(':username', $this->_username);
			$sth->execute();
		}
		catch( PDOException $exception )
		{
			error_log("get_collection_list_with_counts: " . $exception->getMessage());
			throw new Exception("Database unavailable", 503);
		}
		$results = $sth->fetchAll(PDO::FETCH_NUM);
		$sth->closeCursor();
		
		$collections = array();
		$user_collections = 0;
		foreach ($results as $result)
		{
			if (!array_key_exists($result[0], $this->WEAVE_COLLECTION_NAMES) && !$user_collections)
			{
				$this->get_users_collection_list();
				$user_collections = 1;
			}
			
			if (array_key_exists($result[0], $this->WEAVE_COLLECTION_NAMES))
				$result[0] = $this->WEAVE_COLLECTION_NAMES[$result[0]];
			else
				continue;

			$collections[$result[0]] = $result[1];
		}
		return $collections;		
	}

	function get_collection_list_with_all()
	{
		$this->open_connection();

		try
		{
			$select_stmt = 'select collection, max(modified) as timestamp, count(*) as ct from ' . $this->_db_name . ' where username = :username group by collection';
			$sth = $this->_dbh->prepare($select_stmt);
			$sth->bindParam(':username', $this->_username);
			$sth->execute();
		}
		catch( PDOException $exception )
		{
			error_log("get_collection_list_with_counts: " . $exception->getMessage());
			throw new Exception("Database unavailable", 503);
		}
		$results = $sth->fetchAll(PDO::FETCH_NUM);
		$sth->closeCursor();
		
		$collections = array();
		$user_collections = 0;
		foreach ($results as $result)
		{
			if (!array_key_exists($result[0], $this->WEAVE_COLLECTION_NAMES) && !$user_collections)
			{
				$this->get_users_collection_list();
				$user_collections = 1;
			}
			
			if (array_key_exists($result[0], $this->WEAVE_COLLECTION_NAMES))
				$result[0] = $this->WEAVE_COLLECTION_NAMES[$result[0]];
			else
				continue;

			$collections[$result[0]] = array($result[1], $result[2]);
		}
		return $collections;		
	}
	
	function store_object(&$wbos) 
	{
		$this->open_connection();
		
		$insert_stmt = 'insert into ' . $this->_db_name . ' (username, id, collection, parentid, 
						predecessorid, sortindex, modified, payload, payload_size) values ';

		$param_string = '(?, ?, ?, ?, ?, ?, ?, ?, ?)';
		$params = array();
		$values = array();
		
		foreach ($wbos as $wbo)
		{
			$collection = $this->get_collection_id($wbo->collection());

	 		array_push($params, $param_string);
			array_push($values, $this->_username, $wbo->id(), $collection, $wbo->parentid(),
							$wbo->predecessorid(), $wbo->sortindex(), $wbo->modified(), 
							$wbo->payload(), $wbo->payload_size());
		}
		
		$insert_stmt .= implode(',', $params);
		
		$insert_stmt .= ' on duplicate key update parentid = values(parentid), 
						predecessorid = values(predecessorid), sortindex = values(sortindex), 
						modified = values(modified), payload = values(payload), 
						payload_size = values(payload_size)';
		try
		{
			$sth = $this->_dbh->prepare($insert_stmt);
			$sth->execute($values);
			return $sth->rowCount();
		}
		catch( PDOException $exception )
		{
			error_log("store_object: " . $exception->getMessage());
			throw new Exception("Database unavailable", 503);
		}
	}

	function update_object(&$wbo)
	{
		$this->open_connection();

		$update = 'update ' . $this->_db_name . ' set ';
		$params = array();
		$update_list = array();
		
		#make sure we have an id and collection. No point in continuing otherwise
		if (!$wbo->id() || !$wbo->collection())
		{
			error_log('Trying to update without a valid id or collection!');
			return false;
		}
		
		if ($wbo->parentid_exists())
		{
			$update_list[] = "parentid = ?";
			$params[] = $wbo->parentid();
		}	
		
		if ($wbo->predecessorid_exists())
		{
			$update_list[] = "predecessorid = ?";
			$params[] = $wbo->predecessorid();
		}
		
		if ($wbo->sortindex_exists())
		{
			$update_list[] = "sortindex = ?";
			$params[] = $wbo->sortindex();
		}
		
		#Under standard weave semantics, update will not be called if there's no payload. 
		#However, this is included for functional completion
		if ($wbo->payload_exists())
		{
			$update_list[] = "payload = ?";
			$update_list[] = "payload_size = ?";
			$params[] = $wbo->payload();
			$params[] = $wbo->payload_size();
		}
		
		# Don't modify the timestamp on a weight-only change. It's purely for sorting trees.
		if ($wbo->parentid_exists() || $wbo->payload_exists()) 
		{
			#better make sure we have a modified date. Should have been handled earlier
			if (!$wbo->modified_exists())
			{
				error_log("Called update_object with no defined timestamp. Please check");
				$wbo->modified(microtime(1));
			}
			$update_list[] = "modified = ?";
			$params[] = $wbo->modified();

		}
		
		if (count($params) == 0)
		{
			return false;
		}
		
		$update .= join($update_list, ",");

		$update .= " where username = ? and collection = ? and id = ?";
		$params[] = $this->_username;
		
		$collection = $this->get_collection_id($wbo->collection());
		$params[] = $collection;
		
		$params[] = $wbo->id();
		try
		{
			$sth = $this->_dbh->prepare($update);
			$sth->execute($params);
			return $sth->rowCount();
		}
		catch( PDOException $exception )
		{
			error_log("update_object: " . $exception->getMessage());
			throw new Exception("Database unavailable", 503);
		}
	}
	
	function delete_object($collection, $id)
	{
		$this->open_connection();

		try
		{
			$delete_stmt = 'delete from ' . $this->_db_name . ' where username = :username and collection = :collection and id = :id';
			$sth = $this->_dbh->prepare($delete_stmt);

			$sth->bindParam(':username', $this->_username);

			$collectionid = $this->get_collection_id($collection);
			$sth->bindParam(':collection', $collectionid);

			$sth->bindParam(':id', $id);
			$sth->execute();
			return $sth->rowCount();
		}
		catch( PDOException $exception )
		{
			error_log("delete_object: " . $exception->getMessage());
			throw new Exception("Database unavailable", 503);
		}
	}
	
	
	function delete_objects($collection, $id = null, $parentid = null, $predecessorid = null, $newer = null, 
								$older = null, $sort = null, $limit = null, $offset = null, $ids = null, 
								$index_above = null, $index_below = null)
	{
		$this->open_connection();

		$params = array();
		
		$select_stmt = 'delete from ' . $this->_db_name . ' where username = ? and collection = ?';
		$params[] = $this->_username;

		$collectionid = $this->get_collection_id($collection);
		$params[] = $collectionid;
	
		
		if ($id)
		{
			$select_stmt .= " and id = ?";
			$params[] = $id;
		}
		
		if ($ids && count($ids) > 0)
		{
			$qmarks = array();
			$select_stmt .= " and id in (";
			foreach ($ids as $temp)
			{
				$params[] = $temp;
				$qmarks[] = '?';
			}
			$select_stmt .= implode(",", $qmarks);
			$select_stmt .= ')';
		}
		
		if ($parentid)
		{
			$select_stmt .= " and parentid = ?";
			$params[] = $parentid;
		}
		
		if ($predecessorid)
		{
			$select_stmt .= " and predecessorid = ?";
			$params[] = $predecessorid;
		}

		if ($index_above)
		{
			$select_stmt .= " and sortindex > ?";
			$params[] = $index_above;
		}

		if ($index_below)
		{
			$select_stmt .= " and sortindex < ?";
			$params[] = $index_below;
		}
				
		if ($newer)
		{
			$select_stmt .= " and modified > ?";
			$params[] = $newer;
		}
	
		if ($older)
		{
			$select_stmt .= " and modified < ?";
			$params[] = $older;
		}
	
		if ($sort == 'index')
		{
			$select_stmt .= " order by sortindex desc";
		}
		else if ($sort == 'newest')
		{
			$select_stmt .= " order by modified desc";
		}
		else if ($sort == 'oldest')
		{
			$select_stmt .= " order by modified";
		}
		

		if ($limit)
		{
			$limitVal = intval($limit);
			if ($limitVal < 0) {
				throw new Exception("Illegal limit value", 400);
			}
			$select_stmt .= " limit " . $limitVal;
			if ($offset)
			{
				$offsetVal = intval($offset);
				if ($offsetVal < 0) {
					throw new Exception("Illegal offset value", 400);
				}
				$select_stmt .= " offset " . $offsetVal;
			}
		} else if ($offset) {
			throw new Exception("Offset requires limit", 400); 
		}

		try
		{
			$sth = $this->_dbh->prepare($select_stmt);
			$sth->execute($params);
			return $sth->rowCount();
		}
		catch( PDOException $exception )
		{
			error_log("delete_objects: " . $exception->getMessage());
			throw new Exception("Database unavailable", 503);
		}
	}

	function retrieve_object($collection, $id)
	{
		$this->open_connection();

		try
		{
			$select_stmt = 'select * from ' . $this->_db_name . ' where username = :username and collection = :collection and id = :id';
			$sth = $this->_dbh->prepare($select_stmt);
			$sth->bindParam(':username', $this->_username);

			$collectionid = $this->get_collection_id($collection);
			$sth->bindParam(':collection', $collectionid);

			$sth->bindParam(':id', $id);
			$sth->execute();
		}
		catch( PDOException $exception )
		{
			error_log("retrieve_object: " . $exception->getMessage());
			throw new Exception("Database unavailable", 503);
		}
		
		$result = $sth->fetch(PDO::FETCH_ASSOC);
		$wbo = new wbo();
		$wbo->populate($result);
		if ($wbo->validate())
			return $wbo;
		else
			return null;
	}
	
	function retrieve_objects($collection, $id = null, $full = null, $direct_output = null, $parentid = null, 
								$predecessorid = null, $newer = null, 
								$older = null, $sort = null, $limit = null, $offset = null, $ids = null, 
								$index_above = null, $index_below = null)
	{
		$this->open_connection();

		$full_list = $full ? '*' : 'id';
		
		$select_stmt = "select $full_list from " . $this->_db_name . ' where username = ? and collection = ?';
		$params[] = $this->_username;
		$collectionid = $this->get_collection_id($collection);
		$params[] = $collectionid;
		
		
		if ($id)
		{
			$select_stmt .= " and id = ?";
			$params[] = $id;
		}
		
		if ($ids && count($ids) > 0)
		{
			$qmarks = array();
			$select_stmt .= " and id in (";
			foreach ($ids as $temp)
			{
				$params[] = $temp;
				$qmarks[] = '?';
			}
			$select_stmt .= implode(",", $qmarks);
			$select_stmt .= ')';
		}
		
		if ($parentid)
		{
			$select_stmt .= " and parentid = ?";
			$params[] = $parentid;
		}
		
		if ($predecessorid)
		{
			$select_stmt .= " and predecessorid = ?";
			$params[] = $predecessorid;
		}
		
		if ($index_above)
		{
			$select_stmt .= " and sortindex > ?";
			$params[] = $parentid;
		}

		if ($index_below)
		{
			$select_stmt .= " and sortindex < ?";
			$params[] = $parentid;
		}
		
		if ($newer)
		{
			$select_stmt .= " and modified > ?";
			$params[] = $newer;
		}
	
		if ($older)
		{
			$select_stmt .= " and modified < ?";
			$params[] = $older;
		}
	
		if ($sort == 'index')
		{
			$select_stmt .= " order by sortindex desc";
		}
		else if ($sort == 'newest')
		{
			$select_stmt .= " order by modified desc";
		}
		else if ($sort == 'oldest')
		{
			$select_stmt .= " order by modified";
		}
		
		if ($limit)
		{
			$limitVal = intval($limit);
			if ($limitVal < 0) {
				throw new Exception("Illegal limit value", 400);
			}
			$select_stmt .= " limit " . $limitVal;
			if ($offset)
			{
				$offsetVal = intval($offset);
				if ($offsetVal < 0) {
					throw new Exception("Illegal offset value", 400);
				}
				$select_stmt .= " offset " . $offsetVal;
			}
		} else if ($offset) {
			throw new Exception("Offset requires limit", 400);		
		}
		
		try
		{
			$sth = $this->_dbh->prepare($select_stmt);
			$sth->execute($params);
		}
		catch( PDOException $exception )
		{
			error_log("retrieve_collection: " . $exception->getMessage());
			throw new Exception("Database unavailable", 503);
		}

		if ($direct_output)
		{
			$direct_output->set_rowcount($sth->rowCount());
			$direct_output->first();
			while($result = $sth->fetch(PDO::FETCH_ASSOC))
			{
				$wbo = new wbo();
				$wbo->populate($result);
				if (!$full || $wbo->validate())
					$direct_output->output($wbo);
			}
			$direct_output->last();
			return;
		}
			

		$ids = array();
		while ($result = $sth->fetch(PDO::FETCH_ASSOC))
		{
			if ($full)
			{
				$wbo = new wbo();
				$wbo->populate($result);
				if ($wbo->validate())
					$ids[] = $wbo;
			}
			else
				$ids[] = $result['id'];
		}		
		return $ids;
	}
	
	function get_storage_total()
	{
		$this->open_connection();

		try
		{
			$select_stmt = 'select sum(payload_size) from ' . $this->_db_name . ' where username = :username';
			$sth = $this->_dbh->prepare($select_stmt);
			$sth->bindParam(':username', $this->_username);
			$sth->execute();
		}
		catch( PDOException $exception )
		{
			error_log("get_storage_total: " . $exception->getMessage());
			throw new Exception("Database unavailable", 503);
		}
		
		return $sth->fetchColumn();		
	}
	
	function create_user()
	{
		return true; #nothing needs doing on the storage side
	}
	
	function delete_user()
	{
		$this->open_connection();

		try
		{
			$delete_stmt = 'delete from ' . $this->_db_name . ' where username = :username';
			$sth = $this->_dbh->prepare($delete_stmt);
			$sth->bindParam(':username', $this->_username);
			$sth->execute();

			$delete_stmt2 = 'delete from ' . $this->_collection_table_name . ' where userid = :username';
			$sth = $this->_dbh->prepare($delete_stmt2);
			$sth->bindParam(':username', $this->_username);
			$sth->execute();

		}
		catch( PDOException $exception )
		{
			error_log("delete_user: " . $exception->getMessage());
			throw new Exception("Database unavailable", 503);
		}
		return true;

	}

	function heartbeat()
	{
		$this->open_connection();

		try
		{
			$sth = $this->_dbh->prepare('select 1');
			$sth->execute();
		}
		catch( PDOException $exception )
		{
			throw new Exception("Database unavailable");
		}
		$result = $sth->fetchColumn();
		return $result;
	}

}

 ?>