/*
 * =====================================================================================
 * 
 *       Filename:  RedisConfBase.h
 * 
 *    Description:  redis基本配置类，不能实例化
 * 
 *        Version:  1.0
 *        Created:  10/15/2014 04:22:26 PM CST
 *       Revision:  none
 *       Compiler:  gcc
 * 
 *         Author:  szxdlutsun 
 *        Company:  
 * 
 * =====================================================================================
 */

#ifndef  REDISCONFBASE_INC
#define  REDISCONFBASE_INC
#include "Common.h"
#include "HandleRedisReply.h"

using namespace kapalai;

namespace REDIS
{
    class CRedisConfBase
    {
    public:

        int InitReader(const string& strIP, uint32_t ulPort, const struct timeval& stConnTimeOut, const struct timeval& stRWTimeOut);
        int InitWriter(const string& strIP, uint32_t ulPort, const struct timeval& stConnTimeOut, const struct timeval& stRWTimeOut);
        
    protected:

        CRedisConfBase();
        virtual ~CRedisConfBase() throw();


    protected:
        //为了以后可能的读写分离，这里先准备好读写两个实例

        struct timeval m_stReaderConnTimeout;         //连接超时时间
        struct timeval m_stWriterConnTimeout;         //连接超时时间
        struct timeval m_stReaderRWTimeout;         //读写超时时间
        struct timeval m_stWriterRWTimeout;         //读写超时时间

        string 	m_strReadRedisIP ; 
        uint32_t  	m_ulReadRedisPort;
        redisContext* m_ReadContext;    //当前连接的上下文
        HandleReply  m_Reader;          //读句柄

        string 	m_strWriteRedisIP ; 
        uint32_t  	m_ulWriteRedisPort;
        redisContext* m_WriterContext;
        HandleReply m_Writer;           //写句柄
    };

}



#endif   /* ----- #ifndef REDISCONFBASE_INC  ----- */

