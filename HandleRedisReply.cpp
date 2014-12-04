/*
 * =====================================================================================
 *
 *       Filename:  HandleRedisReply.cpp
 *
 *    Description:  处理redis回复的类
 *
 *        Version:  1.0
 *        Created:  08/11/2014 02:40:40 PM CST
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  szxdlutsun 
 *        Company:  
 *
 * =====================================================================================
 */

#include "HandleRedisReply.h"

using namespace REDIS;

redisConnection::redisConnection()
{
    m_vConnArray.clear();
}
redisConnection::~redisConnection()
{
    for(vector<SConnInfo>::iterator it = m_vConnArray.begin(); it != m_vConnArray.end(); it++)
    {
        if(NULL != (*it).Context)
            redisFree((*it).Context);
    }

    m_vConnArray.clear();
}

int redisConnection::ConnToRedis(redisContext*& c, const  string& strHost, uint16_t usPort, const struct timeval&  stConnTimeOut, const struct timeval&  stRWTimeOut)
{
    c = redisConnectWithTimeout(strHost.c_str(), usPort, stConnTimeOut);
    if(c == NULL || c->err) 
    {
        string strMessage = "";
        if(c) 
        {
            strMessage = string("Connection error: ") + c->errstr;
            redisFree(c);
        } 
        else 
        {
            strMessage =  "Connection error: can't allocate redis context";
        }

        throw TC_Redis_Exception("Connect to redis ip=" + strHost + "  port=" + TC_Common::tostr(usPort) + "  message=" + strMessage);   
    }

    //设置读写超时时间
    SetTimeOut(c, stRWTimeOut);

    return 0;
}

int redisConnection::SetTimeOut(redisContext*& c, const struct timeval&  stRWTimeOut)
{
    if(0 == stRWTimeOut.tv_sec && 0 == stRWTimeOut.tv_usec)
    {
        //不要设置超时时间
        return 0;
    }
    if(REDIS_OK != redisSetTimeout(c,stRWTimeOut))
    {
        throw TC_Redis_Exception("set redis read/write timeout on a blocking socket fail");
    }

    return 0;
}

int redisConnection::CreateConn(redisContext*& c, const  string& strHost, uint16_t usPort, const struct timeval&  stConnTimeOut, const struct timeval& stRWTimeOut)
{
    ConnToRedis(c, strHost, usPort, stConnTimeOut, stRWTimeOut);

    SConnInfo stConnInfo = {0};
    stConnInfo.Context = c;
    stConnInfo.strHost = strHost;
    stConnInfo.usPort = usPort;
    stConnInfo.ConnTimeOut = stConnTimeOut;
    stConnInfo.RWTimeOut = stRWTimeOut;
    m_vConnArray.push_back(stConnInfo);

    return 0;
}

int redisConnection::ReConn(redisContext*& c)
{
    if(NULL == c)
    {
        throw TC_Redis_Exception("req ReConn redisContext is null");
    }

    redisContext* tmp = c;

    vector<SConnInfo>::iterator it = m_vConnArray.begin();
    for(; it != m_vConnArray.end(); it++)
    {
        if(tmp == (*it).Context)
            break;
    }

    if(it == m_vConnArray.end())
    {
        throw TC_Redis_Exception("req redisContext is not hold");
    }


    ConnToRedis(c, (*it).strHost, (*it).usPort, (*it).ConnTimeOut, (*it).RWTimeOut);

    redisFree(tmp);

    (*it).Context = c;

    return 0;
}

int redisConnection::DisConn(redisContext*& c)
{
    if(NULL == c)
    {
        throw TC_Redis_Exception("req DisConn redisContext is null");
    }

    redisContext* tmp = c;

    vector<SConnInfo>::iterator it = m_vConnArray.begin();
    for(; it != m_vConnArray.end(); it++)
    {
        if(tmp == (*it).Context)
            break;
    }

    if(it == m_vConnArray.end())
    {
        throw TC_Redis_Exception("req redisContext is not hold");
    }

    redisFree(tmp);

    m_vConnArray.erase(it);

    return 0;
}

/*
int HandleReply::SetReply(const redisReply* Reply)
{
    if(NULL == m_Context || NULL == *m_Context)
    {
        throw TC_Redis_Exception("SetReply obj but curr context is null");
    }

    FreeSingleReply();
    m_SingleReply = Reply;
    return 0;
}
*/

HandleReply::~HandleReply() throw()
{
    FreeSingleReply();
    FreePipeReply();
}

void HandleReply::FreeSingleReply()
{
    if(NULL != m_SingleReply)
    {
        freeReplyObject(m_SingleReply);
        m_SingleReply = NULL;
    }
}

void HandleReply::FreePipeReply()
{
    if(!m_PipeReply.empty())
    {
        for(vector<redisReply*>::iterator it = m_PipeReply.begin(); it != m_PipeReply.end(); it++)
        {
            if(NULL != *it)
            {
                freeReplyObject(*it);
                *it = NULL;
            }
        }

        m_PipeReply.clear();
    }
}

int HandleReply::SetContext(const redisContext** c)
{
    m_Context = (redisContext**)c;
    return 0;
}

int  HandleReply::GetConnection(const string& strHost, uint16_t usPort, const struct timeval&  stConnTimeOut, const struct timeval& stRWTimeOut)
{
    if(NULL == m_Context)
    {
        throw TC_Redis_Exception("can not get Connection for null  Context");
    }

    m_Conn.CreateConn(*m_Context, strHost, usPort, stConnTimeOut, stRWTimeOut);

    return 0;
}


int HandleReply::CheckConn(const redisReply* Reply)
{
    if(NULL == Reply)
    {
        if(NULL == m_Context)
        {
            throw TC_Redis_Exception("can not get Connection for null  Context");
        }

        string strMessage = "Reply is NULL  err = " + TC_Common::tostr((*m_Context)->err) + "  strerr = " + (*m_Context)->errstr;

        m_Conn.ReConn(*m_Context);

        //抛出异常信息
        throw TC_Redis_Exception(strMessage);
    }

    return 0;
}

int HandleReply::SendSingleCmd(const string& strCmd)
{  
    //先要释放以前使用的空间
    FreeSingleReply();

    m_SingleReply = (redisReply*)redisCommand(*m_Context, strCmd.c_str());

    CheckConn(m_SingleReply);

    return 0;
}

int HandleReply::SendSingleCmd(const vector<string>& v)
{  
    //先要释放以前使用的空间
    FreeSingleReply();

    vector<const char *> argv(v.size());
    vector<size_t> argvlen(v.size());
    int32_t j = 0;

    for(vector<string>::const_iterator i = v.begin(); i != v.end(); ++i, ++j)
        argv[j] = i->c_str(), argvlen[j] = i->size();

    m_SingleReply = (redisReply*)redisCommandArgv(*m_Context, argv.size(), &(argv[0]), &(argvlen[0]));

    CheckConn(m_SingleReply);

    return 0;
}

int HandleReply::SendPipeCmd(const vector<string>& vCmd)
{
    //先要释放以前使用的空间
    FreePipeReply();

    //先记录命令数量
    uint16_t usCmdNum = vCmd.size();

    for(uint16_t usIndex=0;usIndex<usCmdNum;usIndex++)
    {
        if(REDIS_OK != redisAppendCommand(*m_Context, vCmd[usIndex].c_str()))
        {
            //出现严重问题，需要重连
            string strMessage = "redisAppendCommand to redis fail  " + TC_Common::tostr((*m_Context)->err) + "strerr = " + (*m_Context)->errstr;
            m_Conn.ReConn(*m_Context);
            throw TC_Redis_Exception(strMessage);
        }
    }

    //取出所有回复
    redisReply* Reply = NULL;  
    for(uint16_t usIndex=0;usIndex<usCmdNum;usIndex++)
    {
        if(REDIS_OK != redisGetReply(*m_Context, (void**)&Reply))
        {
            //出现严重问题，需要重连
            string strMessage = "redisGetReply to redis fail  " + TC_Common::tostr((*m_Context)->err) + "strerr = " + (*m_Context)->errstr;
            m_Conn.ReConn(*m_Context);
            throw TC_Redis_Exception(strMessage);
        }

        m_PipeReply.push_back(Reply);
        Reply = NULL;
    }

    return 0;
}


int HandleReply::SendPipeCmd(const vector< vector<string> >& v)
{
    //先要释放以前使用的空间
    FreePipeReply();

    //先记录命令数量
    uint16_t usCmdNum = v.size();

    for(uint16_t usIndex=0;usIndex<usCmdNum;usIndex++)
    {
        vector<const char *> argv(v[usIndex].size());
        vector<size_t> argvlen(v[usIndex].size());
        int32_t j = 0;

        for(vector<string>::const_iterator i = v[usIndex].begin(); i != v[usIndex].end(); ++i, ++j)
            argv[j] = i->c_str(), argvlen[j] = i->size();

        if(REDIS_OK != redisAppendCommandArgv(*m_Context, argv.size(), &(argv[0]), &(argvlen[0])))
        {
            //出现严重问题，需要重连
            string strMessage = "redisAppendCommandArgv to redis fail  " + TC_Common::tostr((*m_Context)->err) + "strerr = " + (*m_Context)->errstr;
            m_Conn.ReConn(*m_Context);
            throw TC_Redis_Exception(strMessage);
        }
    }

    //取出所有回复
    redisReply* Reply = NULL;  
    for(uint16_t usIndex=0;usIndex<usCmdNum;usIndex++)
    {
        if(REDIS_OK != redisGetReply(*m_Context, (void**)&Reply))
        {
            //出现严重问题，需要重连
            string strMessage = "redisGetReply to redis fail  " + TC_Common::tostr((*m_Context)->err) + "strerr = " + (*m_Context)->errstr;
            m_Conn.ReConn(*m_Context);
            throw TC_Redis_Exception(strMessage);
        }

        m_PipeReply.push_back(Reply);
        Reply = NULL;
    }

    return 0;
}

int HandleReply::GetInteger(int64_t& illRsp)
{
    GetInteger(m_SingleReply, illRsp);
    return 0;
}

int HandleReply::GetInteger(const redisReply* Reply, int64_t& illRsp)
{
    CheckReply(Reply);

    if(REDIS_REPLY_INTEGER != Reply->type)
    {
        throw TC_Redis_Exception("redis return value is not integer type  return type is  = " + TC_Common::tostr(Reply->type));
    }

    illRsp = Reply->integer;

    return 0;
}

int HandleReply::SetExpireSec(const string& strKey, int64_t illSeconds)
{
    if(illSeconds > 0)
    {
        string strCmd = "EXPIRE  " + strKey + "  " + TC_Common::tostr(illSeconds); 
        SendSingleCmd(strCmd);

        int64_t illRet = -1;
        GetInteger(illRet);
        if(1 != illRet)
        {
            throw TC_Redis_Exception("set Expire timeout for " + strKey + "  fail");
        }
    }

    return 0;
}

int HandleReply::GetString(string& strRsp)
{
    int64_t illStrLen = 0;
    GetString(m_SingleReply, strRsp, illStrLen);

    return 0;
}

int HandleReply::GetString(const redisReply* Reply, string& strRsp)
{
    int64_t illStrLen = 0;
    GetString(Reply, strRsp, illStrLen);

    return 0;
}

int HandleReply::GetString(const redisReply* Reply, string& strRsp, int64_t& illStrLen)
{
    CheckReply(Reply);

    if(REDIS_REPLY_STRING == Reply->type)
    {
        illStrLen = Reply->len;
        strRsp = Reply->str;
    }
    else if(REDIS_REPLY_NIL == Reply->type)
    {
        illStrLen = 0;
        strRsp = "NULL";
    }
    else
    {
        throw TC_Redis_Exception("redis return value is not string type  return type is  = " + Reply->type);
    }

    return 0;
}

int HandleReply::GetStatu(string& strRsp)
{
    GetStatu(m_SingleReply, strRsp);

    return 0;
}

int HandleReply::GetStatu(const redisReply* Reply, string& strRsp)
{
    CheckReply(Reply);

    if(REDIS_REPLY_STATUS != Reply->type)
    {
        throw TC_Redis_Exception("redis return value is not status type  return type is  = " + Reply->type);
    }

    strRsp = Reply->str;

    return 0;
}


int HandleReply::GetPipeArray(vector< vector<string> >& vRsp)
{
    for(size_t Index=0;Index<m_PipeReply.size();Index++)
    {
        int64_t illRspSize = 0;
        vector<string> vTmp;
        GetArray(m_PipeReply[Index], vTmp, illRspSize);
        vRsp.push_back(vTmp);
    }

    return 0;
}

int HandleReply::GetArray(vector<string>& vRsp)
{
    int64_t illRspSize = 0;
    GetArray(m_SingleReply, vRsp, illRspSize);

    return 0;
}

int HandleReply::GetArray(const redisReply* Reply, vector<string>& vRsp, int64_t& illRspSize)
{
    CheckReply(Reply);

    if(REDIS_REPLY_ARRAY != Reply->type)
    {
        throw TC_Redis_Exception("redis return value is not array type  return type is  = " + Reply->type);
    }

    illRspSize = Reply->elements;

    for(int64_t i=0;i<illRspSize;i++)
    {
        if(REDIS_REPLY_STRING == (Reply->element)[i]->type)
        {
            vRsp.push_back((Reply->element)[i]->str);
        }
        else if(REDIS_REPLY_NIL == (Reply->element)[i]->type)
        {
            vRsp.push_back("NULL");
        }
        else
        {
            throw TC_Redis_Exception("redis return array value  element is not string type  return type is  = " + (Reply->element)[i]->type);
        }
    }

    return 0;
}

int HandleReply::GetScanValue(vector<string>& vRsp, int64_t& illCursor)
{
    int64_t illRspSize = 0;
    GetScanValue(m_SingleReply, vRsp, illRspSize, illCursor);

    return 0;
}

int HandleReply::GetScanValue(const redisReply* Reply, vector<string>& vRsp, int64_t& illRspSize, int64_t& illCursor)
{
    CheckReply(Reply);

    if(REDIS_REPLY_ARRAY != Reply->type)
    {
        throw TC_Redis_Exception("redis return value is not array type  return type is  = " + Reply->type);
    }

    //取第一个元素；为字符串
    if(REDIS_REPLY_STRING != (Reply->element)[0]->type && REDIS_REPLY_NIL != (Reply->element)[0]->type)
    {
        throw TC_Redis_Exception("redis return array value  element is not string type  return type is  = " + (Reply->element)[0]->type);
    }

    illCursor = strtoll((Reply->element)[0]->str, NULL, 10);


    ///取第二个数组数据
    illRspSize = (Reply->element)[1]->elements;
    struct redisReply **element = (Reply->element)[1]->element; 

    for(int64_t i=0;i<illRspSize;i++)
    {
        if(REDIS_REPLY_STRING == element[i]->type)
        {
            vRsp.push_back(element[i]->str);
        }
        else if(REDIS_REPLY_NIL == element[i]->type)
        {
            vRsp.push_back("NULL");
        }
        else
        {
            throw TC_Redis_Exception("redis return array value  element is not string type  return type is  = " + element[i]->type);
        }
    }

    return 0;
}
