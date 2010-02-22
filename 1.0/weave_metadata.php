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

#Class to abstract out the metadata elements of weave: collection timestamps, counts and storage amounts.
#Parts will be exposed depending on the constants set

require_once 'weave_constants.php';

class WeaveMetadata
{
	private $_memc = null;
	private $_userid = null;
	private $_collections = null;
	private $_datasize = null;
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
				$this->_memc->connect(WEAVE_STORAGE_MEMCACHE_HOST, WEAVE_STORAGE_MEMCACHE_PORT);
			}			
			catch( Exception $exception )
			{
				error_log($exception->getMessage());
				$this->_memc = null;
			}				
		}
	}

	function get_system_quota_display()
	{
		if (defined('WEAVE_QUOTA'))
			return (int)(WEAVE_QUOTA/1024);
		return null;
	}

	function check_quota()
	{
		if (!defined('WEAVE_QUOTA'))
			return;
		
		if ($this->storage_total_get() > WEAVE_QUOTA)
				report_problem("Over Quota", 403); 
	}
	
	function check_timestamp($collection)
	{
		if (array_key_exists('HTTP_X_IF_UNMODIFIED_SINCE', $_SERVER) 
			&& $this->get_max_timestamp($collection) > round($_SERVER['HTTP_X_IF_UNMODIFIED_SINCE'] * 100))
				report_problem(WEAVE_ERROR_NO_OVERWRITE, 412);			
	}

	
	function storage_total_get()
	{
		if ($this->_datasize)
			return $this->_datasize;
			
		if ($this->_memc)
		{
			$datasize = $this->_memc->get('data:' . $this->_userid);
			if ($datasize != null)
			{
				$this->_datasize = $datasize;
				return $datasize;
			}
		}	
		$this->_datasize = $this->_db->get_storage_total();
		$this->storage_total_set();
		return $this->_datasize;
	}
	
	
	function storage_total_flush()
	{
		if ($this->_memc)
			$this->_memc->delete('data:' . $this->_userid);	
	}
	
	function storage_total_set()
	{
		if ($this->_memc && $this->_datasize !== null)
			$this->_memc->set('data:' . $this->_userid, $this->_datasize, false, WEAVE_STORAGE_MEMCACHE_DECAY);	
	}

	function storage_total_add($extra)
	{
		if ($this->_memc && $this->_datasize !== null)
		{
			$this->_datasize = $this->storage_total_get() + $extra;
			$this->_memc->set('data:' . $this->_userid, $this->_datasize, false, WEAVE_STORAGE_MEMCACHE_DECAY);
		}
	}



	function get_max_timestamp($collection)
	{
		if ($this->_memc) #to our advantage to leverage the collection object we'll be updating soon.
		{
			$this->collections_get();
			if ($this->_collections)
				return $this->_collections[$collection];
			else
				return null;
		}
		else
		{
			return $this->_db->get_max_timestamp($collection);
		}
	}
	
	function get_collection_timestamps()
	{
		$this->collections_get();
		return $this->_collections;
	}
	
	function collections_get()
	{
		if ($this->_collections !== null) #already have it
			return;
			
		if ($this->_memc && ($collections = $this->_memc->get('coll:' . $this->_userid)) && is_array($collections))
		{
			$this->_collections = $collections;
			return;
		}	
		
		$collections = $this->_db->get_collection_list_with_timestamps();
		if (is_array($collections))
		{
			#get_collection_list_with_timestamps always returns an array, so we should be guaranteed to have one
			$this->_collections = $collections;
			$this->collections_set();
		}
	}
	
	function collections_update($collection, $timestamp)
	{
		if ($this->_memc)
		{
			if (!$collection || !$timestamp)
				return;
			$this->collections_get();
			$this->_collections[$collection] = $timestamp;
			$this->collections_set();
		}	
	}
	
	function collections_set()
	{
		if ($this->_memc && $this->_collections)
			$this->_memc->set('coll:' . $this->_userid, $this->_collections, false, WEAVE_STORAGE_MEMCACHE_DECAY);	
	}

	function collections_flush()
	{
		if ($this->_memc)
			$this->_memc->delete('coll:' . $this->_userid);	
		$this->_collections = array();
	}

	#we don't use this enough yet to justify memcaching it.
	function get_collection_list_with_counts()
	{
		return $this->_db->get_collection_list_with_counts();
	}
	

}

?>
