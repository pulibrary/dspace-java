<!-- 
  Transforms UMI ETD metadata XML to DSpace Dublin Core XML. 
-->

<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform" version="2.0">
	<xsl:output method="xml" encoding="iso-8859-1" indent="yes"/>

	<xsl:template match="/">
		<xsl:for-each select="DISS_submission">
			<dublin_core>
				<dcvalue element="contributor" qualifier="author">
					<xsl:value-of select="DISS_authorship/DISS_author/DISS_name/DISS_surname"/>, <xsl:value-of select="DISS_authorship/DISS_author/DISS_name/DISS_fname"/>
					<xsl:if test="DISS_authorship/DISS_author/DISS_name/DISS_middle[.!='']">
						<xsl:text>  </xsl:text>
						<xsl:value-of select="DISS_authorship/DISS_author/DISS_name/DISS_middle"/>
					</xsl:if>
				</dcvalue>
				<xsl:for-each select="DISS_description/DISS_advisor">
					<dcvalue element="contributor" qualifier="advisor">
						<xsl:value-of select="./DISS_name/DISS_surname"/>, <xsl:value-of select="./DISS_name/DISS_fname"/>
						<xsl:if test="./DISS_name/DISS_middle[.!='']">
							<xsl:text>  </xsl:text>
							<xsl:value-of select="./DISS_name/DISS_middle"/>
						</xsl:if>
					</dcvalue>
				</xsl:for-each>
				
				<xsl:for-each select="DISS_description/DISS_institution/DISS_inst_contact">
					<dcvalue element="contributor" qualifier="other"><xsl:value-of select="."/> Department</dcvalue>
				</xsl:for-each>
				
				<dcvalue element="title" qualifier="none">
					<xsl:call-template name="stripHTML">
						<xsl:with-param name="outputString">
							<xsl:value-of select="DISS_description/DISS_title"/>
						</xsl:with-param>
					</xsl:call-template>
				</dcvalue>
				<dcvalue element="date" qualifier="issued"><xsl:value-of select="DISS_description/DISS_dates/DISS_comp_date"/></dcvalue>
				<dcvalue element="language" qualifier="iso"><xsl:value-of select="DISS_description/DISS_categorization/DISS_language"/></dcvalue>
				<xsl:call-template name="recurse">
					<xsl:with-param name="parse-string">
						<xsl:value-of select="DISS_description/DISS_categorization/DISS_keyword"/>
					</xsl:with-param>
				</xsl:call-template>
				<!-- have to do this since this field is formatted different ways in different records -->
				<xsl:for-each select="DISS_description/DISS_categorization/DISS_category">
					<xsl:variable name="catdesc" select="DISS_cat_desc"/>
					<xsl:if test="not($catdesc)">
						<dcvalue element="subject" qualifier="classification"><xsl:value-of select="."/></dcvalue>
					</xsl:if>
					<xsl:if test="$catdesc">
						<dcvalue element="subject" qualifier="classification"><xsl:value-of select="$catdesc"/></dcvalue>
					</xsl:if>					
				</xsl:for-each>				
				<dcvalue element="description" qualifier="abstract">
					<xsl:for-each select="DISS_content/DISS_abstract/DISS_para">
						<xsl:call-template name="stripHTML">
							<xsl:with-param name="outputString">
								<xsl:value-of select="."/>
							</xsl:with-param>
						</xsl:call-template>
						
						
						<!-- ## ADD LINEBREAK FOR ALL BUT THE LAST ENTRY -->
						<xsl:if test="position() != last()">
<xsl:text>
								
</xsl:text>							
						</xsl:if>
					</xsl:for-each>
				 </dcvalue>
				 <dcvalue element="type" qualifier="none">Academic dissertations (<xsl:value-of select="DISS_description/DISS_degree"/>)</dcvalue>
				<dcvalue element="relation" qualifier="isformatof">
					<![CDATA[The Mudd Manuscript Library retains one bound copy of each dissertation.  Search for these copies in the library's main catalog: <a href=http://catalog.princeton.edu> catalog.princeton.edu </a>]]>
					</dcvalue>
				<dcvalue element="publisher">Princeton, NJ : Princeton University</dcvalue>
			</dublin_core>
		</xsl:for-each>
	</xsl:template>

	<xsl:template name="recurse">
		<xsl:param name="parse-string"/>
		<xsl:if test="not($parse-string='')">
			 <dcvalue element="subject" qualifier="none">
				<xsl:choose>
					<xsl:when test="contains($parse-string, '; ')"><xsl:value-of select="substring-before($parse-string, '; ' )"/></xsl:when>
					<xsl:when test="contains($parse-string, ', ')"><xsl:value-of select="substring-before($parse-string, ', ' )"/></xsl:when>
					<xsl:otherwise><xsl:value-of select="$parse-string"/></xsl:otherwise>
				</xsl:choose>
			 </dcvalue>
			<xsl:call-template name="recurse">
				<xsl:with-param name="parse-string">
					<xsl:choose>
						<xsl:when test="contains($parse-string, '; ')"><xsl:value-of select="substring-after($parse-string, '; ')"/></xsl:when>
						<xsl:when test="contains($parse-string, ', ')"><xsl:value-of select="substring-after($parse-string, ', ')"/></xsl:when>
						<xsl:otherwise><xsl:value-of select="substring-after($parse-string, '; ')"/></xsl:otherwise>
					</xsl:choose>
				</xsl:with-param>
			</xsl:call-template>
		</xsl:if>
	</xsl:template>
	
	<xsl:template name="globalReplace">
		<xsl:param name="outputString"/>
		<xsl:param name="target"/>
		<xsl:param name="replacement"/>
		<xsl:choose>
			<xsl:when test="contains($outputString,$target)">
				<xsl:value-of select=
					"concat(substring-before($outputString,$target),
					$replacement)"/>
				<xsl:call-template name="globalReplace">
					<xsl:with-param name="outputString" 
						select="substring-after($outputString,$target)"/>
					<xsl:with-param name="target" select="$target"/>
					<xsl:with-param name="replacement" 
						select="$replacement"/>
				</xsl:call-template>
			</xsl:when>
			<xsl:otherwise>
				<xsl:value-of select="$outputString"/>
			</xsl:otherwise>
		</xsl:choose>
	</xsl:template>
	
	<xsl:template name="stripHTML">
		<xsl:param name="outputString"/>
		
		<xsl:variable name="state1">
			<xsl:call-template name="globalReplace">
				<xsl:with-param name="outputString" select="$outputString"/>
				<xsl:with-param name="target" select="'&lt;i&gt;'"/>
				<xsl:with-param name="replacement" select="''"/>
			</xsl:call-template>	
		</xsl:variable>
		
		<xsl:variable name="state2">
			<xsl:call-template name="globalReplace">
				<xsl:with-param name="outputString" select="$state1"/>
				<xsl:with-param name="target" select="'&lt;/i&gt;'"/>
				<xsl:with-param name="replacement" select="''"/>
			</xsl:call-template>
		</xsl:variable>
		
		<xsl:variable name="state3">
			<xsl:call-template name="globalReplace">
				<xsl:with-param name="outputString" select="$state2"/>
				<xsl:with-param name="target" select="'&lt;em&gt;'"/>
				<xsl:with-param name="replacement" select="''"/>
			</xsl:call-template>
		</xsl:variable>
		
		<xsl:variable name="state4">
			<xsl:call-template name="globalReplace">
				<xsl:with-param name="outputString" select="$state3"/>
				<xsl:with-param name="target" select="'&lt;/em&gt;'"/>
				<xsl:with-param name="replacement" select="''"/>
			</xsl:call-template>
		</xsl:variable>
		
		<xsl:variable name="state5">
			<xsl:call-template name="globalReplace">
				<xsl:with-param name="outputString" select="$state4"/>
				<xsl:with-param name="target" select="'&lt;p&gt;'"/>
				<xsl:with-param name="replacement" select="''"/>
			</xsl:call-template>
		</xsl:variable>
		
		<xsl:variable name="state6">
			<xsl:call-template name="globalReplace">
				<xsl:with-param name="outputString" select="$state5"/>
				<xsl:with-param name="target" select="'&lt;/p&gt;'"/>
				<xsl:with-param name="replacement" select="''"/>
			</xsl:call-template>
		</xsl:variable>
		
		<xsl:variable name="state7">
			<xsl:call-template name="globalReplace">
				<xsl:with-param name="outputString" select="$state6"/>
				<xsl:with-param name="target" select="'&lt;sub&gt;'"/>
				<xsl:with-param name="replacement" select="''"/>
			</xsl:call-template>
		</xsl:variable>
		
		<xsl:variable name="state8">
			<xsl:call-template name="globalReplace">
				<xsl:with-param name="outputString" select="$state7"/>
				<xsl:with-param name="target" select="'&lt;/sub&gt;'"/>
				<xsl:with-param name="replacement" select="''"/>
			</xsl:call-template>
		</xsl:variable>
		
		<xsl:variable name="state9">
			<xsl:call-template name="globalReplace">
				<xsl:with-param name="outputString" select="$state8"/>
				<xsl:with-param name="target" select="'&amp;lt;i&amp;gt;'"/>
				<xsl:with-param name="replacement" select="''"/>
			</xsl:call-template>
		</xsl:variable>
		
		<xsl:variable name="state10">
			<xsl:call-template name="globalReplace">
				<xsl:with-param name="outputString" select="$state9"/>
				<xsl:with-param name="target" select="'&lt;br /&gt;'"/>
				<xsl:with-param name="replacement" select="' '"/>
			</xsl:call-template>
		</xsl:variable>
		
		<xsl:variable name="state11">
			<xsl:call-template name="globalReplace">
				<xsl:with-param name="outputString" select="$state10"/>
				<xsl:with-param name="target" select="'&lt;br/&gt;'"/>
				<xsl:with-param name="replacement" select="' '"/>
			</xsl:call-template>
		</xsl:variable>
		
		<xsl:variable name="state12">
			<xsl:call-template name="globalReplace">
				<xsl:with-param name="outputString" select="$state11"/>
				<xsl:with-param name="target" select="'&lt;/sup&gt;'"/>
				<xsl:with-param name="replacement" select="''"/>
			</xsl:call-template>
		</xsl:variable>
		
		<xsl:variable name="state13">
			<xsl:call-template name="globalReplace">
				<xsl:with-param name="outputString" select="$state12"/>
				<xsl:with-param name="target" select="'&lt;sup&gt;'"/>
				<xsl:with-param name="replacement" select="''"/>
			</xsl:call-template>
		</xsl:variable>
		
		<xsl:variable name="state14">
			<xsl:call-template name="globalReplace">
				<xsl:with-param name="outputString" select="$state13"/>
				<xsl:with-param name="target" select="'&amp;lt;/sup&amp;gt;'"/>
				<xsl:with-param name="replacement" select="''"/>
			</xsl:call-template>
		</xsl:variable>
		
		<xsl:variable name="state15">
			<xsl:call-template name="globalReplace">
				<xsl:with-param name="outputString" select="$state14"/>
				<xsl:with-param name="target" select="'&amp;lt;sup&amp;gt;'"/>
				<xsl:with-param name="replacement" select="''"/>
			</xsl:call-template>
		</xsl:variable>
		
		<xsl:call-template name="globalReplace">
			<xsl:with-param name="outputString" select="$state15"/>
			<xsl:with-param name="target" select="'&amp;lt;/i&amp;gt;'"/>
			<xsl:with-param name="replacement" select="''"/>
		</xsl:call-template>
	</xsl:template>
	
	
</xsl:stylesheet>
