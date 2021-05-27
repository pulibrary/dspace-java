<!-- 
    Transforms UMI ETD metadata XML to an author list text file 
-->
<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform" version="2.0">
    <xsl:output method="xml" omit-xml-declaration="yes" encoding="iso-8859-1" indent="yes" />
    <xsl:strip-space elements="*"/>
    <xsl:template match="/">
    	<author>   	
        	<xsl:for-each select="DISS_submission">
		<name>
			<surname><xsl:value-of select="DISS_authorship/DISS_author/DISS_name/DISS_surname"/></surname>
			<fname><xsl:value-of select="DISS_authorship/DISS_author/DISS_name/DISS_fname"/></fname>
			<xsl:if test="DISS_authorship/DISS_author/DISS_name/DISS_middle[.!='']">
				<middle><xsl:value-of select="DISS_authorship/DISS_author/DISS_name/DISS_middle"/></middle>
			</xsl:if>
		</name>
        		<phone>
        			<xsl:value-of select="DISS_authorship/DISS_author/DISS_contact[@type = 'current']/DISS_phone"/>        			
        		</phone>
		<current_address>
			<addrline><xsl:value-of select="DISS_authorship/DISS_author/DISS_contact[@type = 'current']/DISS_address/DISS_addrline"/></addrline>
			<city><xsl:value-of select="DISS_authorship/DISS_author/DISS_contact[@type = 'current']/DISS_address/DISS_city"/></city>
			<pcode><xsl:value-of select="DISS_authorship/DISS_author/DISS_contact[@type = 'current']/DISS_address/DISS_pcode"/></pcode>
			<country><xsl:value-of select="DISS_authorship/DISS_author/DISS_contact[@type = 'current']/DISS_address/DISS_country"/></country>
		</current_address>
        		<email><xsl:value-of select="DISS_authorship/DISS_author/DISS_contact[@type = 'current']/DISS_email"/></email>
        		<diss_title><xsl:value-of select="DISS_description/DISS_title"/></diss_title> 
        	</xsl:for-each>
    	</author>
    </xsl:template>

</xsl:stylesheet>
