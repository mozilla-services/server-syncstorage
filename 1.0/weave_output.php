<?php


#The datasets we might be dealing with here are too large for sticking it all into an array, so
#we need to define a direct-output method for the storage class to use. If we start producing multiples
#(unlikely), we can put them in their own class.

class WBOOutput  
{
	private $_full = false;
	private $_comma_flag = false;
	private $_rowcount = 0;
	private $_output_format = 'json';
	
	function __construct ($full = false)
	{
		$this->_full = $full;
		if (array_key_exists('HTTP_ACCEPT', $_SERVER)
			&& !preg_match('/\*\/\*/', $_SERVER['HTTP_ACCEPT'])
			&& !preg_match('/application\/json/', $_SERVER['HTTP_ACCEPT']))
		{
			if (preg_match('/application\/whoisi/', $_SERVER['HTTP_ACCEPT']))
			{
				header("Content-type: application/whoisi");
				$this->_output_format = 'whoisi';
			}
			elseif (preg_match('/application\/newlines/', $_SERVER['HTTP_ACCEPT']))
			{
				header("Content-type: application/newlines");
				$this->_output_format = 'newlines';
			}
			
		}
	}
		
	function set_rowcount($rowcount)
	{
		$this->_rowcount = $rowcount;
	}
	
	function first()
	{
		if ($this->_rowcount)
			header('X-Weave-Records: ' . $this->_rowcount);

		if ($this->_output_format == 'json')
			echo '[';
	}
	
	function output($wbo)
	{		
		if ($this->_output_format == 'newlines')
		{
			if ($this->_full)
			{
				echo preg_replace('/\n/', '\u000a', $wbo->json());
			}
			else
				echo json_encode($wbo->id());
			echo "\n";
		}
		elseif ($this->_output_format == 'whoisi')
		{
			if ($this->_full)
			{
				$output = $wbo->json();
			}
			else
				$output = json_encode($wbo->id());
			echo pack('N', mb_strlen($output, '8bit')) . $output;
		}
		else
		{
			if ($this->_comma_flag) { echo ','; } else { $this->_comma_flag = true; }
			if ($this->_full)
			{
				echo $wbo->json();
			}
			else
				echo json_encode($wbo->id());
		}
	}
	
	function last()
	{
		if ($this->_output_format == 'json')
			echo ']';
	}
}

?>