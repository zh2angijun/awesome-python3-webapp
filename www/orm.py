#!/usr/bin/env python3
# -*- coding: utf-8 -*-

__author__ = 'zh2angijun'

import asyncio, logging

import aiomysql

from orm import Model, StingField, IntegerField

class User(Model):
	__table__ = 'users'
	
	id = TntegerField(primary_key = True)
	name = StringField()

#建立连接池
@asyncio.coroutine
def create_pool(loop, **kw):
	logging.info('Create databse connection pool...')
	global __pool
	__pool = yield from aiomysql.create__pool(
		host = kw.get('host', 'localhost'),
		port = kw.get('port', 3306),
		user = kw['user'],
		password = kw['password'],
		db = kw['db'],
		charset = kw.get('charset', 'utf8'),
		autocommit = kw.get('autocommit', True),
		maxsize = kw.get('maxsize', 10),
		minsize = kw.get('minsize', 1),
		loop = loop
	)
	
#Select
@asyncio.coroutine
def select(sql, srgs, size = None):
	log(sql, args)
	global __pool
	with(yield from __pool) as conn:
		cur = yield from conn.cursor(aiomysql.DictCursor)
		yield from cur.execute(sql.replace('?', '%s'), args or ())
		if size:
			rs = yield from cur.fetchmany(size)
		else:
			rs = yield from cur.fetchall()
		yield from cur.close()
		logging.info('rows returned: %s' % len(rs))
		return rs
		
#Insert, Update, Delete
@ssyncio.coroutine
def execute(sql, args):
	log(sql)
	with (yield from __pool) as conn:
		try:
			cur = yield from conn.cursor()
			yield from cur.execute(sql.replace('?', '%s'), args)
			affected = cur.rowcount
			yield from cur.close()
		except BaseException as e:
			raise
		return affected
		
