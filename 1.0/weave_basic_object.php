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

require_once 'weave_constants.php';

class wbo
{
	var $wbo_hash = array();
	var $_collection;
	var $_error = array();
	
	function extract_json(&$json)
	{
		
		$extracted = is_string($json) ? json_decode($json, true) : $json;

		#need to check the json was valid here...
		if ($extracted === null)
		{
			$this->_error[] = "unable to extract from json";
			return 0;
		}
		
		#must have an id, or all sorts of badness happens. However, it can be added later
		if (array_key_exists('id', $extracted))
		{
			$this->id($extracted['id']);
		}
		
		if (array_key_exists('parentid', $extracted))
		{
			$this->parentid($extracted['parentid']);
		}
		
		if (array_key_exists('predecessorid', $extracted))
		{
			$this->predecessorid($extracted['predecessorid']);
		}

		if (array_key_exists('sortindex', $extracted))
		{
			$this->sortindex($extracted['sortindex']);
		}
		
		if (array_key_exists('payload', $extracted))
		{
			$this->payload($extracted['payload']);
		}
		return 1;
	}
	
	function populate(&$datahash)
	{
		$this->id($datahash['id']);
		$this->collection($datahash['collection']);
		$this->parentid($datahash['parentid']);
		$this->modified($datahash['modified']);
		$this->predecessorid($datahash['predecessorid']);
		$this->sortindex($datahash['sortindex']);
		$this->payload($datahash['payload']);
	}

	function id($id = null)
	{
		if (!is_null($id)) { $this->wbo_hash['id'] = $id; }
		return array_key_exists('id', $this->wbo_hash) ?  $this->wbo_hash['id'] : null;
	}
	
	function collection($collection = null)
	{
		if (!is_null($collection)){ $this->_collection = $collection; }
		return $this->_collection;
	}
	
	function parentid($parentid = null)
	{
		if (!is_null($parentid)){ $this->wbo_hash['parentid'] = $parentid; }
		return array_key_exists('parentid', $this->wbo_hash) ?  $this->wbo_hash['parentid'] : null;
	}
	
	function parentid_exists()
	{
		return array_key_exists('parentid', $this->wbo_hash);
	}
	
	function predecessorid($predecessorid = null)
	{
		if (!is_null($predecessorid)){ $this->wbo_hash['predecessorid'] = $predecessorid; }
		return array_key_exists('predecessorid', $this->wbo_hash) ?  $this->wbo_hash['predecessorid'] : null;
	}
	
	function predecessorid_exists()
	{
		return array_key_exists('predecessorid', $this->wbo_hash);
	}
	
	function modified($modified = null)
	{
		if (!is_null($modified)){ $this->wbo_hash['modified'] = round((float)$modified, 2); }
		return array_key_exists('modified', $this->wbo_hash) ?  $this->wbo_hash['modified'] : null;
	}
	
	function modified_exists()
	{
		return array_key_exists('modified', $this->wbo_hash);
	}
	
	function payload($payload = null)
	{
		if (!is_null($payload)){ $this->wbo_hash['payload'] = $payload; }
		return array_key_exists('payload', $this->wbo_hash) ?  $this->wbo_hash['payload'] : null;
	}
	
	function payload_exists()
	{
		return array_key_exists('payload', $this->wbo_hash);
	}

	function payload_size()
	{
		return mb_strlen($this->wbo_hash['payload'], '8bit');
	}
	
	function sortindex($index = null)
	{
		if (!is_null($index)){ $this->wbo_hash['sortindex'] = (int)$index; }
		return array_key_exists('sortindex', $this->wbo_hash) ?  $this->wbo_hash['sortindex'] : null;
	}

	function sortindex_exists()
	{
		return array_key_exists('sortindex', $this->wbo_hash);
	}
	
		
	function validate()
	{
		
		if (!$this->id() || strlen($this->id()) > 64)
		{ $this->_error[] = "invalid id"; }

		if ($this->parentid_exists() && strlen($this->parentid()) > 64)
		{ $this->_error[] = "invalid parentid"; }

		if ($this->predecessorid_exists() && strlen($this->predecessorid()) > 64)
		{ $this->_error[] = "invalid predecessorid"; }

		if (!is_numeric($this->modified()))
		{ $this->_error[] = "invalid modified date"; }
		
		if (!$this->modified())
		{ $this->_error[] = "no modification date"; }

		if (!$this->_collection || strlen($this->_collection) > 64)
		{ $this->_error[] = "invalid collection"; }
		
		if ($this->sortindex_exists() && !is_numeric($this->sortindex()))
		{ $this->_error[] = "invalid sortindex"; }
		
		if ($this->payload_exists())
		{
			if (!is_string($this->wbo_hash['payload']))
			{ $this->_error[] = "payload needs to be json-encoded"; }
			else if (WEAVE_PAYLOAD_MAX_SIZE && strlen($this->wbo_hash['payload']) > WEAVE_PAYLOAD_MAX_SIZE)
			{ $this->_error[] = "payload too large"; }
		}
		
		return !$this->get_error();
	}

	function get_error()
	{
		return $this->_error;
	}
	
	function clear_error()
	{
		$this->_error = array();
	}
	
	function json()
	{
		$this->wbo_hash['modified'] /= 100; #stupid hack to output timestamps in decimal
		return json_encode($this->wbo_hash);
		$this->wbo_hash['modified'] *= 100;
	}
}


?>