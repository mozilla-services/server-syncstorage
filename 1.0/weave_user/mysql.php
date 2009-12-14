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
#	Anant Narayanan (anant@kix.in)
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
	
require_once 'weave_user/base.php';
require_once 'weave_constants.php';

#Mysql version of the authentication object.
#Note that this object does not contain any database setup information. It assumes that the mysql
#instance is already fully configured as part of the weave-registration server

class WeaveAuthentication implements WeaveAuthenticationBase
{
	var $_dbh;
	var $_username = null;
	var $_alert = null;
	
	function __construct($username) 
	{
		$this->open_connection();
		$this->_username = $username;
	}

	function open_connection() 
	{ 
		$hostname = WEAVE_MYSQL_AUTH_HOST;
		$dbname = WEAVE_MYSQL_AUTH_DB;
		$dbuser = WEAVE_MYSQL_AUTH_USER;
		$dbpass = WEAVE_MYSQL_AUTH_PASS;
		
		try
		{
			$this->_dbh = new PDO('mysql:host=' . $hostname . ';dbname=' . $dbname, $dbuser, $dbpass);
			$this->_dbh->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
		}
		catch( PDOException $exception )
		{
				error_log($exception->getMessage());
				throw new Exception("Database unavailable", 503);
		}
		return 1;
	}
	
	function get_connection()
	{
		return $this->_dbh;
	}


	function authenticate_user($password)
	{
		try
		{
			$select_stmt = 'select id, status, alert from users where username = :username and md5 = :md5';
			$sth = $this->_dbh->prepare($select_stmt);
			$pwhash = md5($password);
			$sth->bindParam(':username', $this->_username);
			$sth->bindParam(':md5', $pwhash);
			$sth->execute();
		}
		catch( PDOException $exception )
		{
			error_log("authenticate_user: " . $exception->getMessage());
			throw new Exception("Database unavailable", 503);
		}

		if (!$result = $sth->fetch(PDO::FETCH_ASSOC))
		{
			return null;
		}
		
		$this->_alert = $result['alert'];
		
		if ($result['status'] != 1)
			return null;
			
		return $result['id'];
	}

	
	function get_user_alert()
	{
		return $this->_alert;
	}
	
}



?>
