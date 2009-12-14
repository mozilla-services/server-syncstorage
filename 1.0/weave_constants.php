<?php
	if (array_key_exists('HTTP_HOST', $_SERVER) && file_exists($_SERVER['HTTP_HOST'] . '_constants.php'))
		require_once $_SERVER['HTTP_HOST'] . '_constants.php';

	if (file_exists('default_constants.php'))
		require_once 'default_constants.php';


?>
