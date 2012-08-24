<?xml version="1.0" encoding="UTF-8"?>
<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform" xmlns:xs="http://www.w3.org/2001/XMLSchema" exclude-result-prefixes="xs" version="2.0">
  <!-- 
    Документировалка меппингов.
    Выводит в html таблицу со смещённым количеством столбцов
    -->
  <xsl:output method="html" indent="yes"/>
  <xsl:param name="maxdepth" select="number(substring(concat(
    '16'[count(current()/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*)>0],
    '15'[count(current()/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*)>0],
    '14'[count(current()/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*)>0],
    '13'[count(current()/*/*/*/*/*/*/*/*/*/*/*/*/*/*)>0],
    '12'[count(current()/*/*/*/*/*/*/*/*/*/*/*/*/*)>0],
    '11'[count(current()/*/*/*/*/*/*/*/*/*/*/*/*)>0],
    '10'[count(current()/*/*/*/*/*/*/*/*/*/*)>0],
    '09'[count(current()/*/*/*/*/*/*/*/*/*)>0],
    '08'[count(current()/*/*/*/*/*/*/*/*)>0],
    '07'[count(current()/*/*/*/*/*/*/*)>0],
    '06'[count(current()/*/*/*/*/*/*)>0],
    '05'[count(current()/*/*/*/*/*)>0],
    '04'[count(current()/*/*/*/*)>0],
    '03'[count(current()/*/*/*)>0],
    '02'[count(current()/*/*)>0],
    '01'[count(current()/*)>0])
    ,1,2)
    )"/>
  <xsl:template match="/">
    <html>
      <head/>
      <body><xsl:comment>max-depth: <xsl:value-of select="$maxdepth"/></xsl:comment>
        <table border="1px">
          <thead>
            <tr>
              <th>1c</th>
              <th>2d</th>
              <th>PI</th>
              <xsl:if test="$maxdepth > 0"><th>1</th></xsl:if>
              <xsl:if test="$maxdepth > 1"><th>2</th></xsl:if>
              <xsl:if test="$maxdepth > 2"><th>3</th></xsl:if>
              <xsl:if test="$maxdepth > 3"><th>4</th></xsl:if>
              <xsl:if test="$maxdepth > 4"><th>5</th></xsl:if>
              <xsl:if test="$maxdepth > 5"><th>6</th></xsl:if>
              <xsl:if test="$maxdepth > 6"><th>7</th></xsl:if>
              <xsl:if test="$maxdepth > 7"><th>8</th></xsl:if>
              <xsl:if test="$maxdepth > 8"><th>9</th></xsl:if>
              <xsl:if test="$maxdepth > 9"><th>10</th></xsl:if>
              <xsl:if test="$maxdepth > 10"><th>11</th></xsl:if>
              <th></th>
            </tr>
          </thead>
          <tbody>
          <xsl:apply-templates select="*" mode="t1"><xsl:with-param name="lev" select="1"/></xsl:apply-templates>
          </tbody>
        </table>
      </body>
    </html>
  </xsl:template>
  
  <xsl:template match="*" mode="t1">
    <xsl:param name="lev" as="xs:decimal"/>
    <tr bgcolor="yellow">
      <td>1c</td>
      <td>2d</td>
      <td colspan="{$lev}"></td>
      <td colspan="{$maxdepth - $lev +1}"><xsl:value-of select="name(current())"/></td>
      <td><xsl:value-of select="text()"/></td>
    </tr>
    <xsl:for-each select="@*">
      <tr bgcolor="darksalmon">
        <td>1c</td>
        <td>2d</td>
        <td colspan="{$lev}"></td>
        <td colspan="{$maxdepth - $lev + 1}">@<xsl:value-of select="name(current())"/></td>
        <td></td>
      </tr>
    </xsl:for-each>
    <xsl:apply-templates select="*" mode="t1"><xsl:with-param name="lev" select="$lev+1"/></xsl:apply-templates>
  </xsl:template>

  
</xsl:stylesheet>
