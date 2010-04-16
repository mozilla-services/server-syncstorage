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

#
#create table users
#(
# id int(11) NOT NULL PRIMARY KEY auto_increment
# username varchar(32),
# password_hash varbinary(128),
# email varbinary(64),
# status tinyint(4) default '1',
# alert text,
# reset varbinary(32) default null,
#) engine=InnoDB;
#

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

	/**
	 * Generate a SSHA password hash using the given password and optional 
	 * salt.  If not supplied, the salt will be generated randomly
	 *
	 * @param   string $password Cleartext password
	 * @param   string $salt     Hashing salt
	 * @returns string SSHA password hash
	 */
	function generateSSHAPassword($password, $salt=null)
	{
		if (setlocale(LC_CTYPE, "UTF8", "en_US.UTF-8") == false) 
			throw new Exception("Database Unavailable", 503);

		// see also: http://blog.coenbijlsma.nl/2009/01/17/php-and-ssha-ldap-passwords/
		if (null === $salt) {
			if (function_exists('mcrypt_create_iv')) {
				$salt = mcrypt_create_iv(8, MCRYPT_DEV_RANDOM);
			} else {
			   mt_srand((double)microtime()*1000000);
			   $salt = pack("CCCCCCCC", 
					   mt_rand(), mt_rand(), mt_rand(), mt_rand(), 
					   mt_rand(), mt_rand(), mt_rand(), mt_rand());
			}
		}
		$ssha = "{SSHA-256}" . 
			base64_encode( hash('sha256', $password . $salt, true) . $salt);
		return $ssha;
	}

	/**
	 * Determine whether the given password is valid for the given SSHA
	 * password hash.
	 *
	 * @param   string $password  Cleartext password
	 * @param   string $ssha_hash SSHA password hash
	 * @returns boolean
	 */
	function validateSSHAPassword($password, $ssha_hash)
	{
		$tag_len   = strlen('{SSHA-256}');
		$salt	   = substr(base64_decode(substr($ssha_hash, $tag_len)), 32);
		$test_hash = $this->generateSSHAPassword($password, $salt);
		return ($ssha_hash == $test_hash);
	}

	/**
	 * Hash a password using the latest password hashing method (eg SSHA)
	 *
	 * @param   string $password Cleartext password
	 * @returns string Password hash
	 */
	function hash_password($password)
	{
		return $this->generateSSHAPassword($password);
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
		return true;
	}
	
	function get_connection()
	{
		return $this->_dbh;
	}

	function update_password($password)
	{
		if (!$this->_username)
		{
			throw new Exception(WEAVE_ERROR_INVALID_USERNAME, 404);
		}
		if (!$password)
		{
			throw new Exception(WEAVE_ERROR_MISSING_PASSWORD, 404);
		}
		
		if (!$this->user_exists())
			throw new Exception("User not found", 404);
			
		try
		{
			$insert_stmt = 'update users set password_hash = :password_hash where username = :username';
			$sth = $this->_dbh->prepare($insert_stmt);
			$sth->bindParam(':username', $this->_username);
			$phash = $this->hash_password($password);
			$sth->bindParam(':password_hash', $phash);
			$sth->execute();
		}
		catch( PDOException $exception )
		{
			error_log("update_password: " . $exception->getMessage());
			throw new Exception("Database unavailable", 503);
		}
		return true;
	
	}
	
	function user_exists() 
	{
		try
		{
			$select_stmt = 'select count(*) from users where username = :username';
			$sth = $this->_dbh->prepare($select_stmt);
			$sth->bindParam(':username', $this->_username);
			$sth->execute();
		}
		catch( PDOException $exception )
		{
			error_log("user_exists: " . $exception->getMessage());
			throw new Exception("Database unavailable", 503);
		}

		$result = $sth->fetchColumn();
		return $result;
	}

	/**
	 * Authenticate the current user with the given password.
	 *
	 * Initially attempts to validate the password using the current latest 
	 * password hashing method (SSHA), but will fallback through MD5 and 
	 * server-salt SSHA.  If a fallback is valid, the user's password is
	 * updated to the latest method.
	 *
	 * @param   string $password Cleartext password
	 * @returns mixed - null on invalid, user ID on valid.
	 */
	function authenticate_user($password)
	{
		$result = null;
		try
		{
			$select_stmt = '
				SELECT id, password_hash, status, alert 
				FROM users 
				WHERE username=?
			';
			$sth = $this->_dbh->prepare($select_stmt);
			$sth->execute(array($this->_username));
			$result = $sth->fetch(PDO::FETCH_ASSOC);
			$sth->closeCursor();

			if (!$result) { 
				// User not found.
				return null; 
			} else if ($result['status'] != 1) {
				// User disabled.
				return null;
			} else if (strpos($result['password_hash'], '{SSHA-256}') !== false) {
				// Looks like a {SSHA-256} password, so try validating it.
				if (!$this->validateSSHAPassword($password, $result['password_hash'])) {
					return null;
				}
			} else {
				// This might be a legacy password hash, so try fallbacks...
				if (defined('WEAVE_MD5_FALLBACK') && WEAVE_MD5_FALLBACK &&
						($result['password_hash'] == md5($password)) ) {
					// Looks like a valid MD5 hash, so accept it but update it.
					$this->update_password($password);
				} else if (defined('WEAVE_SHA_SALT')) {
					// We have a SHA salt, so try generating a hash with it.
					$p_array = str_split($password, (floor(strlen($password)/2))+1);
					$sha_hash = hash('sha256', $p_array[0].WEAVE_SHA_SALT.$p_array[1]);
					if ($result['password_hash'] == $sha_hash) {
						// Looks like a valid SHA hash, so accept it but update it.
						$this->update_password($password);
					} else {
						// Ran out of legacy fallbacks, so bail.
						return null;
					}
				} else {
					// Ran out of legacy fallbacks, so bail.
					return null;
				}
			}
		}
		catch( PDOException $exception )
		{
			error_log("authenticate_user: " . $exception->getMessage());
			throw new Exception("Database unavailable", 503);
		}
		
		$this->_alert = $result['alert'];
		
		return $result['id'];
	}
	
	function get_user_alert()
	{
		return $this->_alert;
	}
	
}

/* vim: set noexpandtab */ 
