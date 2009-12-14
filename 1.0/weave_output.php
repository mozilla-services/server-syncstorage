<?php


#The datasets we might be dealing with here are too large for sticking it all into an array, so
#we need to define a direct-output method for the storage class to use. If we start producing multiples
#(unlikely), we can put them in their own class.

class WBOOutput  
{
	private $_full = false;
	private $_comma_flag = false;
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
	
	function output($sth)
	{
		if (($rowcount = $sth->rowCount()) > 0)
		{
			header('X-Weave-Records: ' . $rowcount);
		}
		if ($this->_output_format == 'newlines')
		{
			return $this->output_newlines($sth);
		}
		elseif ($this->_output_format == 'whoisi')
		{
			return $this->output_whoisi($sth);
		}
		else
		{
			return $this->output_json($sth);
		}
	}
	
	function output_json($sth)
	{
		echo '[';
		
		while ($result = $sth->fetch(PDO::FETCH_ASSOC))
		{
			if ($this->_comma_flag) { echo ','; } else { $this->_comma_flag = true; }
			if ($this->_full)
			{
				$wbo = new wbo();
				$wbo->populate($result);
				echo $wbo->json();
			}
			else
				echo json_encode($result{'id'});
		}

		echo ']';
		return 1;
	}

	function output_whoisi($sth)
	{		
		while ($result = $sth->fetch(PDO::FETCH_ASSOC))
		{
			if ($this->_full)
			{
				$wbo = new wbo();
				$wbo->populate($result);
				$output = $wbo->json();
			}
			else
				$output = json_encode($result{'id'});
			echo pack('N', mb_strlen($output, '8bit')) . $output;
		}
		return 1;
	}

	function output_newlines($sth)
	{		
		while ($result = $sth->fetch(PDO::FETCH_ASSOC))
		{
			if ($this->_full)
			{
				$wbo = new wbo();
				$wbo->populate($result);
				echo preg_replace('/\n/', '\u000a', $wbo->json());
			}
			else
				echo json_encode($result{'id'});
			echo "\n";
		}
		return 1;
	}
}

?>