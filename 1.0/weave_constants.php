<?php
	if (array_key_exists('HTTP_HOST', $_SERVER))
	{
		$host = preg_replace('/[^a-z0-9\.\-]/i', '', $_SERVER['HTTP_HOST']);
		if (file_exists($host . '_constants.php'))
			require_once $_SERVER['HTTP_HOST'] . '_constants.php';
	}
	
	if (file_exists('default_constants.php'))
		require_once 'default_constants.php';


?>
