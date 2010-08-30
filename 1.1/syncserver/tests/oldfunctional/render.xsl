<?xml version="1.0" encoding="UTF-8"?>
<xsl:stylesheet version='1.0'
        xmlns:xsl='http://www.w3.org/1999/XSL/Transform'
        xmlns:xhtml="http://www.w3.org/1999/xhtml"
        xmlns:xforms="http://www.w3.org/2002/xforms"
    xmlns:ts="http://www.w3c.org/MarkUp/Forms/XForms/Test/11">
        
        <xsl:output method="xml" encoding="UTF-8" indent="yes" omit-xml-declaration="no" xalan:indent-amount="2" xmlns:xalan="http://xml.apache.org/xslt"/>
        
    <xsl:param name="dir"></xsl:param>    

  <xsl:template match="testsuite">
    <html xmlns="http://www.w3.org/1999/xhtml"  
			xmlns:ts="http://www.w3c.org/MarkUp/Forms/XForms/Test/11"
    exclude-result-prefixes="ts">
    <head>
				<style>
				BODY {font:9pt Arial,sans-serif}
				TD {font:9pt Arial,sans-serif}
				.heading TD {font:10pt bold Arial, sans-serif}
				.innerCellFailed {background-color:#ffd0d0}
				.innerCellPassed {color:#D0D0D0}
				.inner130 {width:130px}
				.ctl {text-decoration:none;color:#808080;padding-right:8px}
				</style>

        <title>Unit Test Results</title>
				<script>
				function expand(d) {
					document.getElementById(d).style.display = "none";
					document.getElementById(d + "_full").style.display = "block";
				}
				</script>

    </head>
    <body>

        <table cellpadding="0" cellspacing="1" border="0">
            <tr class="heading">
                <td><b>Test Case Name</b></td>
                <td><b>Status</b></td>
            </tr>
      
        <xsl:apply-templates select="*"/>
              </table>
        </body>
   </html>

    </xsl:template>
        
        

<xsl:template match="testcase">
		<xsl:variable name="name" select="@name"/>
		<tr class="outer">              
			<td valign="top" class="inner130"><xsl:value-of select="@name"/></td>
			<xsl:choose>
			<xsl:when test="failure">
				<td class="innerCellFailed">
					<div style='height:14px;overflow:hidden'><xsl:attribute name='id'><xsl:value-of select="@name"/></xsl:attribute><pre style='margin-top:0px'><a class='ctl'><xsl:attribute name="href">javascript:expand('<xsl:value-of select="@name"/>')</xsl:attribute>+</a> <xsl:value-of select="failure"/></pre></div>
					<div style='display:none'><xsl:attribute name='id'><xsl:value-of select="@name"/>_full</xsl:attribute><pre style='margin-top:0px'><a class='ctl'><xsl:attribute name="href">javascript:expand('<xsl:value-of select="@name"/>')</xsl:attribute>+</a> <xsl:value-of select="failure"/></pre></div>
					</td>
			</xsl:when>
			<xsl:otherwise>
				<td class="innerCellPassed">OK</td>

			</xsl:otherwise>
			</xsl:choose>
		</tr>
		
</xsl:template>
 
    
    <xsl:template match="statusSummary"/> 
    <xsl:template match="profile"/> 

</xsl:stylesheet>
