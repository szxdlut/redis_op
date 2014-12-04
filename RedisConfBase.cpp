/*
 * =====================================================================================
 *
 *       Filename:  RedisConfBase.cpp
 *
 *    Description:  redis基本配置类，不能实例化
 *
 *        Version:  1.0
 *        Created:  10/15/2014 04:21:55 PM CST
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  szxdlutsun 
 *        Company:  
 *
 * =====================================================================================
 */
#include "RedisConfBase.h"

using namespace REDIS;

CRedisConfBase::CRedisConfBase():m_Reader(), m_Writer()
{
    memset(&m_stReaderConnTimeout, 0, sizeof(m_stReaderConnTimeout));
    memset(&m_stWriterConnTimeout, 0, sizeof(m_stWriterConnTimeout));
    memset(&m_stReaderRWTimeout, 0, sizeof(m_stReaderRWTimeout));
    memset(&m_stWriterRWTimeout, 0, sizeof(m_stWriterRWTimeout));

    m_strReadRedisIP.clear();
    m_strWriteRedisIP.clear();

    m_ulReadRedisPort = 0;
    m_ulWriteRedisPort = 0;

    m_ReadContext = NULL;
    m_WriterContext = NULL;
}

CRedisConfBase::~CRedisConfBase() throw ()
{
    /*
    if(NULL != m_ReadContext)
    {
        redisFree(m_ReadContext);          
    }

    if(NULL != m_WriterContext)
    {
        redisFree(m_WriterContext);          
    }
    */
}


int CRedisConfBase::InitReader(const string& strIP, uint32_t ulPort, const struct timeval& stConnTimeOut, const struct timeval& stRWTimeOut)
{
    m_strReadRedisIP = strIP;
    m_ulReadRedisPort = ulPort;
    m_stReaderConnTimeout = stConnTimeOut;
    m_stReaderRWTimeout = stRWTimeOut;

    m_ReadContext = NULL;
    m_Reader.SetContext((const redisContext**)&m_ReadContext);

    m_Reader.GetConnection(m_strReadRedisIP, m_ulReadRedisPort, m_stReaderConnTimeout, m_stReaderRWTimeout);

    return 0;
}

int CRedisConfBase::InitWriter(const string& strIP, uint32_t ulPort, const struct timeval& stConnTimeOut, const struct timeval& stRWTimeOut)
{
    m_strWriteRedisIP = strIP;
    m_ulWriteRedisPort = ulPort;
    m_stWriterConnTimeout = stConnTimeOut;
    m_stWriterRWTimeout = stRWTimeOut;

    m_WriterContext = NULL;
    m_Writer.SetContext((const redisContext**)&m_WriterContext);

    m_Writer.GetConnection(m_strWriteRedisIP, m_ulWriteRedisPort, m_stWriterConnTimeout, m_stWriterRWTimeout);

    return 0;
}

