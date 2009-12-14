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

function get_auth_object()
{
	switch(WEAVE_AUTH_ENGINE)
	{
		case 'mysql':
			return new WeaveAuthenticationMysql();
		case 'sqlite':
			return new WeaveAuthenticationSqlite();
		case 'mozilla':
			return new WeaveAuthenticationMozilla();
		case 'htaccess':
		case 'none':
		case '':
			return new WeaveAuthenticationNone();
		default:
			throw new Exception("Unknown authentication type", 503);
	}				
}


interface WeaveAuthentication
{
	function __construct($dbh = null);

	function open_connection();

	function get_connection();

	function authenticate_user($username, $password);

	function get_user_alert();
}

#Dummy object for no-auth and .htaccess setups
class WeaveAuthenticationNone implements WeaveAuthentication
{
	function __construct($dbh = null)
	{
	}

	function open_connection()
	{
		return 1;
	}
	
	function get_connection()
	{
		return null;
	}
	
	function authenticate_user($username, $password)
	{
		return $username;
	}
	
	function get_user_alert()
	{
		return "";
	}

}




#Mysql version of the above.
#Note that this object does not contain any database setup information. It assumes that the mysql
#instance is already fully configured

class WeaveAuthenticationMysql implements WeaveAuthentication
{
	var $_dbh;
	var $_alert = null;
	
	function __construct($dbh = null) 
	{
		if (!$dbh)
		{
			$this->open_connection();
		}
		elseif ($dbh == 'no_connect')
		{
			# do nothing. No connection.
		}
		else
		{
			$this->_dbh = $no_connect;
		}
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


	function authenticate_user($username, $password) #auth user may be different from user, so need the username here
	{
		try
		{
			$select_stmt = 'select id, location, status, alert from users where username = :username and md5 = :md5';
			$sth = $this->_dbh->prepare($select_stmt);
			$pwhash = md5($password);
			$sth->bindParam(':username', $username);
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
			return 0;
			
		if ($result['location'] && $result['location'] != $_SERVER['HTTP_HOST'])
			return 0;
			
		return $result['id'];
	}

	
	function get_user_alert()
	{
		return $this->_alert;
	}
	
}




#Sqlite version of the object
class WeaveAuthenticationSqlite implements WeaveAuthentication
{
	var $_dbh;
	var $_alert;
	
	function __construct($dbh = null)
	{
		if (!$dbh)
		{
			$this->open_connection();
		}
		elseif ($dbh == 'no_connect')
		{
			# do nothing. No connection.
		}
		else
		{
			$this->_dbh = $dbh;
		}
	}
	
	function open_connection()
	{
		$db_name = WEAVE_SQLITE_AUTH_DIRECTORY . '/_users';
		
		try
		{
			$this->_dbh = new PDO('sqlite:' . $db_name);
			$this->_dbh->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
		}
		catch( PDOException $exception )
		{
			throw new Exception("Database unavailable", 503);
		}
	}

	
	function get_connection()
	{
		return $this->_dbh;
	}


	function authenticate_user($username, $password) 
	{
		try
		{
			$select_stmt = 'select id, location, status, alert from users where username = :username and md5 = :md5';
			$sth = $this->_dbh->prepare($select_stmt);
			$sth->bindParam(':username', $username);
			$sth->bindParam(':md5', md5($password));
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

		if (!status)
			return 0;
			
		if ($result['location'] && $result['location'] != $_SERVER['HTTP_HOST'])
			return 0;

		return $result['id'];
	}

	function get_user_alert()
	{
		return $this->_alert;
	}
	
}

# LDAP version of Authentication
class WeaveAuthenticationMozilla implements WeaveAuthentication
{
	var $_conn;
	var $_alert;
	
	private function authorize() {
		if (!ldap_bind($this->_conn, WEAVE_LDAP_AUTH_USER.",".
			WEAVE_LDAP_AUTH_DN, WEAVE_LDAP_AUTH_PASS))
			throw new Exception("Database Unavailable", 503);
	}
	
 	private function constructUserDN($user) {
		/* This is specific to our Weave cluster */
		if (WEAVE_LDAP_AUTH_DN == "dc=mozilla") {
			$md = md5($user);
			$a1 = substr($md, 0, 5);
			$a2 = substr($md, 1, 4);
			$a3 = substr($md, 2, 3);
			$a4 = substr($md, 3, 2);
			$a5 = substr($md, 4, 1);
			
			$dn = WEAVE_LDAP_AUTH_USER_PARAM_NAME."=$user,";
			$dn .= "dc=$a1,dc=$a2,dc=$a3,dc=$a4,dc=$a5,".WEAVE_LDAP_AUTH_DN;
			return $dn;
		}
		
		return WEAVE_LDAP_AUTH_USER_PARAM_NAME."=$user,".WEAVE_LDAP_AUTH_DN;
	}
	
	private function getUserAttribute($user, $attr)
	{
		$this->authorize();
		$dn = $this->constructUserDN($user);
		$re = ldap_read($this->_conn, $dn, "objectClass=*", array($attr));
		return ldap_get_attributes($this->_conn,
			ldap_first_entry($this->_conn, $re));
	}
	
	function __construct($conn = null)
	{
		if (!$conn)
		{
			$this->open_connection();
		}
		else
		{
			$this->_conn = $conn;
		}
	}

	function open_connection()
	{
		$this->_conn = ldap_connect(WEAVE_LDAP_AUTH_HOST);
		if (!$this->_conn)
			throw new Exception("Cannot contact LDAP server", 503);

		ldap_set_option($this->_conn, LDAP_OPT_PROTOCOL_VERSION, 3);
		
		/*
		if (WEAVE_LDAP_USE_TLS) {
			if (!ldap_start_tls($this->_conn))
				throw new Exception("Cannot establish TLS connection", 503);
		}
		*/
		return 1;
	}

	function get_connection()
	{
		return $this->_conn;
	}
 	
	function authenticate_user($username, $password)
	{
		$dn = $this->constructUserDN($username);
		
		// Check if assigned node is same as current host
		$nd = "";
		$va = $this->getUserAttribute($username, "primaryNode");
		$id = $this->getUserAttribute($username, "uidNumber");
		for ($i = 0; $i < $va["primaryNode"]["count"]; $i++)
		{
			$node = $va["primaryNode"][$i];
			if (substr($node, 0, 6) == "weave:") {
				$nd = substr($node, 6);
				break;
			}
		}
	
		if (trim($nd) != $_SERVER['HTTP_HOST'])
			return 0;
		
		if (ldap_bind($this->_conn, $dn, $password))
			return $id['uidNumber'][0];
			
		return 0;
	}


	function get_user_alert()
	{
		return $this->_alert;
	}
	
}

?>
