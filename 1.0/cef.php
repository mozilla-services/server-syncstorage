<?php

class CommonEventFormat
{

	function __construct($vendor, $product, $deviceVersion, $version = 0, $filehandle = 'syslog')
	{
		$this->_vendor = preg_replace('/\|/', '\\\|', $vendor);
		$this->_product = preg_replace('/\|/', '\\\|', $product);
		$this->_deviceVersion = preg_replace('/\|/', '\\\|', $deviceVersion);
		$this->_version = preg_replace('/\|/', '\\\|', $version);
		$this->_filehandle = $filehandle;
	}

	function logMessage($message)
	{
		if (!(get_class($message) == 'CommonEventFormatMessage' || is_subclass_of($message, 'CommonEventFormatMessage')))
			throw new Exception("Message is not CommonEventFormatMessage or derivative");
		
		$sig = preg_replace('/\|/', '\\\|', $message->signature);
		$name = preg_replace('/\|/', '\\\|', $message->name);
		$sev = preg_replace('/\|/', '\\\|', $message->severity);
		
		$body = "CEF:" . implode("|", array($this->_version, $this->_vendor, $this->_product,
									$this->_deviceVersion, $sig, $name,
									$sev, $message->extensionsToString()));
		
		
		if ($this->_filehandle == 'syslog')
			syslog(LOG_NOTICE, implode(" ", array(date("M d H:i:s"), php_uname('n'), $body)));
		else if ($this->_filehandle)
			fwrite($this->_filehandle, implode(" ", array(date("%M %d %H:%i:%s"), php_uname('n'), $body)));
		else
			return $body;
	}
}

class CommonEventFormatMessage
{
	private $data = array('extension' => array());
	
	function __construct($signature, $name, $severity, $extension = array())
	{
		#prepopulate with some specific data
		$this->data['extension']['cs1Label'] = 'requestClientApplication';
		$this->data['extension']['cs1'] = empty($_SERVER['HTTP_USER_AGENT']) ? 'none' : $_SERVER['HTTP_USER_AGENT'];
		$this->data['extension']['requestMethod'] = $_SERVER['REQUEST_METHOD'];
		$this->data['extension']['request'] = 'https://' . $_SERVER['HTTP_HOST'] . $_SERVER['REQUEST_URI'];
		$this->data['extension']['src'] = get_source_ip();
		$this->data['extension']['dst'] = $_SERVER['SERVER_ADDR'];
		
		$this->signature = $signature;
		$this->name = $name;
		$this->severity = $severity;
		foreach ($extension as $k => $v)
			$this->data['extension'][$k] = $v;
	}

	function __set($name, $val)
	{
		if ($name == 'signature')
			$this->data['signature'] = $val;
		elseif ($name == 'name')
			$this->data['name'] = $val;
		elseif ($name =='severity')
		{
			if (!is_int($val))
				throw new Exception("Severity must be an integer");
			elseif ($val < 0 || $val > 10)
				throw new Exception("Severity must be between 0 and 10");
			$this->data['severity'] = $val;
		}
		elseif (strpos($name, '|' ) !== false)
			throw new Exception('extension key may not contain a |');
		else
			$this->data['extension'][$name] = $val;
		
	}
	
	function __get($name)
	{
		if (array_key_exists($name, $this->data))
			return $this->data[$name];
		elseif(array_key_exists($name, $this->data['extension']))
			return $this->data['extension'][$name];
		else
			return null;
	}
	
	function extensionsToString()
	{
		$extension_strings = array();
		foreach ($this->data['extension'] as $k => $v)
		{
			$extension_strings[]  = $k . '=' . preg_replace('/\|/', '\\\|', $v);
		}
		return implode(" ", $extension_strings);
	}
}


?>
