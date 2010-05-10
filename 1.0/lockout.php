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
	
require_once 'weave_constants.php';


class WeaveLockout
{		
	private $_username;
	private $_memc = null;
	private $_lockout = null;
	
	function __construct($username) 
	{
		$this->_username = $username;
		$this->open_connection();
		$this->_lockout = null;
	}

	function open_connection() 
	{		
		$this->_memc = new Memcache;
		$hosts = explode(":", WEAVE_STORAGE_MEMCACHE_HOST);
		try
		{
			foreach ($hosts as $host)
				$this->_memc->addServer($host, WEAVE_STORAGE_MEMCACHE_PORT);
		}			
		catch( Exception $exception )
		{
			error_log("memcache addServer: " . $exception->getMessage());
			throw new Exception("Database unavailable", 503);
		}				
	}
	
	function is_locked()
	{
		global $cef;
		if ($this->get_lockout() >= WEAVE_STORAGE_LOCKOUT_COUNT)
		{
			if ($cef)
			{
				$message = new CommonEventFormatMessage(WEAVE_CEF_ACCOUNT_LOCKED, 'Account Lockout Hit', 1, 
											array('suser' => $this->_username));
				$cef->logMessage($message);
			}
			return true;
		}	

		return false;
	}
	
	function increment_lockout()
	{
		$this->get_lockout();
		$this->_lockout++;
		$this->set_lockout();
	}
	
	function get_lockout()
	{
		if (!defined('WEAVE_STORAGE_LOCKOUT_COUNT') || !WEAVE_STORAGE_LOCKOUT_COUNT)
		{
			return true;
		}
		
		if (!$this->_username || !$this->_memc) 
		{
			error_log("get_lockout (memcache): no username or memcache connection");
			throw new Exception("Database unavailable", 503);			
		}
		try
		{
			$this->_lockout = $this->_memc->get('lockout:' . $this->_username);
		}
		catch (Exception $e)
		{
			error_log("get_lockout (memcache): error on read");
			$this->_lockout = 0;
		}
		return $this->_lockout;
	}
	
	function set_lockout()
	{
		if (!defined('WEAVE_STORAGE_LOCKOUT_COUNT') || !WEAVE_STORAGE_LOCKOUT_COUNT)
		{
			return true;
		}

		if (!$this->_username || !$this->_memc) 
		{
			error_log("set_lockout (memcache): no username or memcache connection");
			throw new Exception("Database unavailable", 503);			
		}		

		try
		{
			$this->_memc->set('lockout:' . $this->_username, $this->_lockout, 0, WEAVE_STORAGE_LOCKOUT_DECAY);	
		}
		catch (Exception $e)
		{
			error_log("set_lockout (memcache): error on write");
		}
		return true;
	}
}

 ?>