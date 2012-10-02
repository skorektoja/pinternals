package com.pinternals.diffo.api;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.sql.SQLException;
import java.text.ParseException;
import java.util.HashMap;

import org.xml.sax.SAXException;

public interface IDiffo {

	/**
	 * Проверяет БД на непротиворечивость, установлено в ассертах или изредка напрямую
	 * @throws SQLException
	 */
	public boolean validatedb() throws SQLException;

	public abstract boolean createdb() throws SQLException,
			ClassNotFoundException;

	/**
	 * Open diffo.db
	 * 
	 * @return
	 * @throws ClassNotFoundException
	 * @throws SQLException
	 */
	public boolean opendb() throws ClassNotFoundException,
			SQLException;

	/**
	 * Если число распределённых страниц больше 0, считает базу существующей
	 * 
	 * @return
	 * @throws SQLException
	 */
	public boolean isDbExist() throws SQLException;

	public boolean start_session() throws SQLException;

	public void finish_session() throws SQLException;

	public void closedb() throws SQLException;

	public boolean refresh(String sid, String url, String user, String password)
	throws MalformedURLException, SQLException, IOException, SAXException, ParseException, InterruptedException;

//	public HashMap<String,String> readTransportNames(long ref)
//	throws SQLException, UnsupportedEncodingException;
}