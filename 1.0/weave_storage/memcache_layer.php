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
	
require_once 'weave_storage/base.php';
require_once 'weave_storage/' . WEAVE_STORAGE_ENGINE . '.php';
require_once 'weave_basic_object.php';


class WeaveMemcache implements WeaveStorageBase
{
	private $_username;
	private $_dbh = null;
	private $_memc = null;
	private $_collections = null;
	private $_tabs = null;
	private $_writes = null;
	
	function __construct($username) 
	{
		$this->_username = $username;
		$this->_dbh = new WeaveStorage($username);
		$this->open_connection();
	}

	function open_connection() 
	{		
		$this->_memc = new Memcache;
		try
		{
			$this->_memc->pconnect(WEAVE_STORAGE_MEMCACHE_HOST, WEAVE_STORAGE_MEMCACHE_PORT);
		}			
		catch( Exception $exception )
		{
			error_log("memcache open_connection: " . $exception->getMessage());
			throw new Exception("Database unavailable", 503);
		}				
	}
	
	function get_connection()
	{
		return $this->_dbh;
	}

	function begin_transaction()
	{
		return $this->_dbh->begin_transaction();
	}

	function commit_transaction()
	{
		return $this->_dbh->commit_transaction();
	}
	
	function get_collection_id($collection)
	{
		return $this->_dbh->get_collection_id($collection);
	}

	function store_collection_id($collection)
	{
		return $this->_dbh->store_collection_id($collection);		
	}

	function get_collection_name($collection_id)
	{		
		return $this->_dbh->get_collection_name($collection_id);		
	}

	function get_users_collection_list()
	{
		return $this->_dbh->get_users_collection_list();
	}
	
	
	function get_max_timestamp($collection)
	{

		if (!$collection)
			return null;
		
		$this->collections_get();
		if (array_key_exists($collection, $this->_collections))
		{
			return $this->_collections[$collection][0];
		}
		return null;
	}

	function get_collection_list()
	{		
		$this->collections_get();

		return array_keys($this->_collections);
	}

	function get_collection_list_with_timestamps()
	{
		$this->collections_get();
		
		$result = array();
		foreach ($this->_collections as $k => $v)
		{
			$result[$k] = $v[0];
		}
		
		return $result;
	}
	
	function get_collection_list_with_counts()
	{
		$this->collections_get();
		
		$result = array();
		foreach ($this->_collections as $k => $v)
		{
			$result[$k] = $v[1];
		}
		
		return $result;
	}
	
	function store_object(&$wbos) 
	{
		if (!$wbos)
			return 0;
		
		$affected = 0;
		$this->collections_get();
			
		if ($wbos[0]->collection() == 'tabs')
		{
			$this->tabs_get();
			
			foreach ($wbos as $wbo)
			{
				if (!array_key_exists($wbo->id(), $this->_tabs) || $wbo->payload() != $this->_tabs[$wbo->id()][1])
					$affected++;
				$this->_tabs[$wbo->id()] = array($wbo->modified(), $wbo->payload());
			}
			$this->tabs_set();
			if ($affected)
				$this->collections_update('tabs', $wbos[0]->modified(), count($this->_tabs));
		
		}
		else
		{
			$payload_total = 0;
			foreach ($wbos as $wbo)
			{
				$payload_total += $wbo->payload_size();
			}
			$this->add_to_write_quota($payload_total);
			
			$affected = $this->_dbh->store_object($wbos);
			if ($affected)
			{
				$count = count($wbos);
				
				#this logic isn't perfect, but handles most duplicate situations. Bleah.
				if ($affected < $count)
					$affected = $count;
				else if ($affected > $count)
					$affected = $count - ($affected - $count);
					
				
				$this->collections_update($wbos[0]->collection(), $wbos[0]->modified(), 
						(array_key_exists($wbos[0]->collection(), $this->_collections) ? 
							$this->_collections[$wbos[0]->collection()][1] : 0) + $affected);
			}
		}
		$this->collections_set();
		return true;
	
	}

	function update_object(&$wbo)
	{
		if (!$wbo)
			return 0;
		
		$affected = 0;
		
		$this->collections_get();
		if ($wbo->collection() == 'tabs')
		{
			$this->tabs_get();
			if (!array_key_exists($wbo->id(), $this->_tabs))
				return true;
				
			if ($wbo->payload_exists())
			{
				$this->_tabs[$wbo->id()] = array($wbo->modified(), $wbo->payload());
				$affected = 1;
			}
			$this->tabs_set();
			$this->collections_update('tabs', $wbo->modified(), count($this->_tabs));
		}
		else
		{
			$this->add_to_write_quota($wbo->payload_size());
			$affected = $this->_dbh->update_object($wbo);		
			if ($affected)
				$this->collections_update($wbo->collection(), $wbo->modified());
		}
		return $affected;		
	}
	
	function delete_object($collection, $id)
	{
		global $storage_time;
		$affected = 0;

		$this->collections_get();
		$id = (string)$id;
		if ($collection == 'tabs')
		{
			$this->tabs_get();
			if (!$this->_tabs || !is_array($this->_tabs) || !array_key_exists($id, $this->_tabs))
				return true;
			
			$new_tabs = array();
			foreach ($this->_tabs as $k => $tab)
			{
				if ($k != $id)
					$new_tabs[$k] = $tab;
				else
					$affected++;
			}
			$this->_tabs = $new_tabs;
			$this->tabs_set();

			if ($affected)
				$this->collections_update('tabs', $storage_time, count($this->_tabs));					
		}
		else
		{
			$affected = $this->_dbh->delete_object($collection, $id);		
			if ($affected && array_key_exists($collection, $this->_collections))
				$this->collections_update($collection, $storage_time, $this->_collections[$collection][1] - $affected);
		}
		return $affected;
	}
	
	
	function delete_objects($collection, $id = null, $parentid = null, $predecessorid = null, $newer = null, 
								$older = null, $sort = null, $limit = null, $offset = null, $ids = null, 
								$index_above = null, $index_below = null)
	{
		global $storage_time;
		$affected = 0;
		
		$this->collections_get();
		if ($collection == 'tabs')
		{
			$this->tabs_get();
			if (!$this->_tabs)
				return true;
			
			$new_tabs = array();
			foreach ($this->_tabs as $k => $tab)
			{
				if (($id && $k != $id) || ($newer && $newer >= $tab[0]) || ($older && $older <= $tab[0])
					 || ($ids && !array_key_exists($k, $ids)))
					$new_tabs[$k] = $tab;
				else
					$affected++;
			}
			
			if (count($this->_tabs) == count($new_tabs))
				return true; #nothing has changed
				
			$this->_tabs = $new_tabs;
			$this->tabs_set();

			$this->collections_update('tabs', $storage_time, count($new_tabs));
		}
		else
		{
			$affected = $this->_dbh->delete_objects($collection, $id, $parentid, $predecessorid, $newer, 
									$older, $sort, $limit, $offset, $ids, 
									$index_above, $index_below);		
			
			if ($affected && array_key_exists($collection, $this->_collections))
				$this->collections_update($collection, $storage_time, $this->_collections[$collection][1] - $affected);
		}
		return $affected;
	}

	function retrieve_object($collection, $id)
	{
		if ($collection == 'tabs')
		{
			$this->tabs_get();
			if (!$this->_tabs)
				return null;

			if (!array_key_exists($id, $this->_tabs))
				return null;
			
			$wbo = new wbo();
			$wbo->id($id);
			$wbo->collection('tabs');
			$wbo->modified($this->_tabs[$id][0]);
			$wbo->payload($this->_tabs[$id][1]);
			return $wbo;
					
		}
		else
		{
			return $this->_dbh->retrieve_object($collection, $id);
		}
	}
	
	function retrieve_objects($collection, $id = null, $full = null, $direct_output = null, $parentid = null, 
								$predecessorid = null, $newer = null, 
								$older = null, $sort = null, $limit = null, $offset = null, $ids = null, 
								$index_above = null, $index_below = null)
	{
		if ($collection == 'tabs')
		{
			$this->tabs_get();
			if (!$this->_tabs)
				return array();
				
			$wbos = array();
			foreach ($this->_tabs as $k => $tab)
			{
				if (($id && $k != $id) || ($newer && $newer >= $tab[0]) || ($older && $older <= $tab[0]) 
					|| ($ids && !array_key_exists($k, $ids)))
					continue;
				$wbo = new wbo();
				$wbo->id($k);
				$wbo->collection('tabs');
				$wbo->modified($tab[0]);
				$wbo->payload($tab[1]);
				$wbos[] = $wbo;
			}
			
			if ($direct_output)
			{
				$direct_output->set_rowcount(count($this->_tabs));
				$direct_output->first();
				foreach($wbos as $wbo)
				{
					if (!$full || $wbo->validate())
						$direct_output->output($wbo);
				}
				$direct_output->last();
				return;
			}
			return $wbos;
		}
		else
		{
			return $this->_dbh->retrieve_objects($collection, $id, $full, $direct_output, $parentid, 
								$predecessorid, $newer, $older, $sort, $limit, $offset, $ids, 
								$index_above, $index_below);
		}
	}
	
	function get_storage_total()
	{
		return $this->_dbh->get_storage_total();		
	}
	
	function create_user()
	{
		return true; #nothing needs doing on the storage side
	}
	
	function delete_user()
	{
		$this->_dbh->delete_user();
		$this->collections_flush();
		$this->tabs_flush();	
		return true;
	}

	function heartbeat()
	{
		return $this->_dbh->heartbeat();
	}
	
	function tabs_get()
	{
		if ($this->_tabs && is_array($this->_tabs))
			return;
		
		if ($this->_memc && ($tabs = $this->_memc->get('tabs:' . $this->_username)) && is_array($tabs))
		{
			$this->_tabs = $tabs;
			return;
		}
		
		$this->_tabs = array();
		return;
	}
	
	function tabs_set()
	{
		if ($this->_memc && $this->_tabs)
			$this->_memc->set('tabs:' . $this->_username, $this->_tabs, true, WEAVE_STORAGE_MEMCACHE_DECAY);	
	}
	
	function tabs_flush()
	{
		if ($this->_memc)
			$this->_memc->delete('tabs:' . $this->_username);	
	}
	
	function collections_get()
	{
		if ($this->_collections !== null) #already have it
			return;
			
		if ($this->_memc && ($collections = $this->_memc->get('coll:' . $this->_username)) && is_array($collections))
		{
			$this->_collections = $collections;
			return;
		}	
		
		$collections = $this->_dbh->get_collection_list_with_all();
		if (is_array($collections))
		{
			#get_collection_list_with_timestamps always returns an array, so we should be guaranteed to have one
			$this->_collections = $collections;
			$this->tabs_get();
			if ($this->_tabs)
			{
				$modified = 0;
				foreach ($this->_tabs as $tab)
				{
					if ($tab[0] > $modified)
						$modified = $tab[0];
				}
				$this->_collections['tabs'] = array($modified, (string)count($this->_tabs));
			}
			$this->collections_set();
		}
		else
		{
			error_log("collections_get (memcache): collection from db not an array");
			throw new Exception("Database unavailable", 503);			
		}

	}
	function add_to_write_quota($total)
	{
		if (!defined('WEAVE_QUOTA_WRITE_CAP'))
			return;
			
		list($date, $volume) = $current_total = $this->_memc->get('write_vol:' . $this->_username);
		$now = date("Ymd");
		if ($now > $date)
		{
			$date = $now;
			$volume = $total;
		}
		else
			$volume += $total;
		
		
		if ($volume > WEAVE_QUOTA_WRITE_CAP)
			throw new Exception("Over write quota", 503);			

		$this->_memc->set('write_vol:' . $this->_username, array($date, $volume), true, WEAVE_STORAGE_MEMCACHE_DECAY);	
		return $volume;
		
	}
	
	function collections_set()
	{
		if ($this->_memc && $this->_collections)
			$this->_memc->set('coll:' . $this->_username, $this->_collections, true, WEAVE_STORAGE_MEMCACHE_DECAY);	
	}

	function collections_update($collection, $modified = null, $total = null)
	{
		$this->collections_get();
		if ($modified !== null)
			$this->_collections[$collection][0] = $modified;
		if ($total !== null)
			$this->_collections[$collection][1] = (string)$total;
		$this->collections_set();

	}
	function collections_flush()
	{
		if ($this->_memc)
			$this->_memc->delete('coll:' . $this->_username);	
		$this->_collections = array();
	}

}

 ?>