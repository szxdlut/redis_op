/*
 * =====================================================================================
 *
 *       Filename:  RedisBaseOp.cpp
 *
 *    Description:  redis基本操作的封装
 *
 *        Version:  1.0
 *        Created:  10/30/2014 11:08:42 AM CST
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  szxdlutsun 
 *        Company:  
 *
 * =====================================================================================
 */
#include "RedisBaseOp.h"

using namespace REDIS;

RedisBaseOp::RedisBaseOp()
{
    
}

RedisBaseOp::~RedisBaseOp() throw()
{

}

int RedisBaseOp::GetHashItem(HandleReply& Reply, const string& strKey, vector<string>& vFieldValue)
{
    if(strKey.empty())
    {
        CCommon::HandleThrow(RF_REDIS_OP_ERR); 
    }

    string strCmd = "HGETALL   " + strKey;
    Reply.SendSingleCmd(strCmd);

    Reply.GetArray(vFieldValue);

    return 0;
}

int RedisBaseOp::GetString(HandleReply& Reply, const string& strKey, string& strValue)
{
    if(strKey.empty())
    {
        CCommon::HandleThrow(RF_REDIS_OP_ERR); 
    }

    string strCmd = "GET  " + strKey;


    Reply.SendSingleCmd(strCmd);

    Reply.GetString(strValue);

    return 0;
}

int RedisBaseOp::GetHashField(HandleReply& Reply, const string& strKey, const vector<string>& vField, vector<string>& vValue)
{
    if(strKey.empty() || vField.empty())
    {
        CCommon::HandleThrow(RF_REDIS_OP_ERR); 
    }

    vector<string> argv;
    argv.push_back("HMGET");
            
    argv.push_back(strKey);

    vector<string>::const_iterator it = vField.begin();
    while(it != vField.end())
    {
        argv.push_back(*it);
        it++;
    }

    Reply.SendSingleCmd(argv);

    Reply.GetArray(vValue);

    return 0;
}

int RedisBaseOp::CheckKeyExists(HandleReply& Reply, const string* pstrKey, bool* pRsp)
{
    if((*pstrKey).empty())
    {
        CCommon::HandleThrow(RF_REDIS_OP_ERR); 
    }

    string strCmd = "EXISTS  "  +  *pstrKey;
    int64_t illRet = 0;
    Reply.SendSingleCmd(strCmd);
    Reply.GetInteger(illRet);
    if(1 == illRet)
    {
        *pRsp = true;
    }
    else
    {
        *pRsp = false;
    }

    return 0;
}

int RedisBaseOp::DelKeys(HandleReply& Reply, const vector<string>& vKeys) 
{
    //检查是否设置字段
    if(vKeys.empty())
    {
        CCommon::HandleThrow(RF_REDIS_OP_ERR); 
    }

    vector<string> argv;
    argv.push_back("DEL");

    vector<string>::const_iterator it = vKeys.begin();
    while(it != vKeys.end())
    {
        argv.push_back(*it);
        it++;
    }

    Reply.SendSingleCmd(argv);

    //得到删除的数量
    int64_t illDelNum = 0;
    Reply.GetInteger(illDelNum);

    return 0;
}

int RedisBaseOp::InsertHashItem(HandleReply& Reply, const string& strKey, const map<string, string>& mValue, int64_t illSeconds)
{
    //检查是否设置字段
    if(mValue.empty() || strKey.empty())
    {
        CCommon::HandleThrow(RF_REDIS_OP_ERR); 
    }

    vector<string> argv;
    argv.push_back("hmset");
    argv.push_back(strKey);

    map<string, string>::const_iterator it = mValue.begin();
    while(it != mValue.end())
    {
        if((it->first).empty())
        {
            CCommon::HandleThrow(RF_REDIS_OP_ERR); 
        }
        argv.push_back(it->first);

        if((it->second).empty())
        {
            CCommon::HandleThrow(RF_REDIS_OP_ERR); 
        }
        argv.push_back(it->second);

        it++;
    }

    Reply.SendSingleCmd(argv);

    string strStatu;
    Reply.GetStatu(strStatu);
    if(0 != strcasecmp("ok", strStatu.c_str()))
    {
        CCommon::HandleThrow(RF_REDIS_OP_ERR); 
    }

    //设置过期时间
    Reply.SetExpireSec(strKey, illSeconds);

    return 0;
}

int RedisBaseOp::RemoveFromList(HandleReply& Reply, const string& strKey, const vector<string>& vValue)
{
    //检查是否设置字段
    if(strKey.empty() || vValue.empty())
    {
        CCommon::HandleThrow(RF_REDIS_OP_ERR); 
    }

    vector<string> argv;
    argv.push_back("LREM");
    argv.push_back(strKey);
    argv.push_back("0");

    vector<string>::const_iterator it = vValue.begin();
    while(it != vValue.end())
    {
        argv.push_back(*it);
        it++;
    }

    Reply.SendSingleCmd(argv);

    //得到删除的数量
    int64_t illDelNum = 0;
    Reply.GetInteger(illDelNum);

    return 0;
}

int RedisBaseOp::BPopFromList(HandleReply& Reply, const vector<string>& vKeys, vector<string>& vValue, emListDirec Direc, int64_t illTimeOut)
{
    if(vKeys.empty())
    {
        CCommon::HandleThrow(RF_REDIS_OP_ERR); 
    }

    vector<string> argv;
    switch(Direc)
    {
    case LEFE:
        {
            argv.push_back("BLPOP");
            break;
        }
    case RIGHT:
        {
            argv.push_back("BRPOP");
            break;
        }
    default:
        {
            CCommon::HandleThrow(RF_REDIS_OP_ERR); 
        }
    }

    vector<string>::const_iterator it = vKeys.begin();
    while(it != vKeys.end())
    {
        argv.push_back(*it);
        it++;
    }

    argv.push_back(TC_Common::tostr(illTimeOut));

    Reply.SendSingleCmd(argv);

    Reply.GetArray(vValue);

    return 0;
}

int RedisBaseOp::InsertListItem(HandleReply& Reply, const string& strKey, const vector<string>& vValue, emListDirec Direc, int64_t illSeconds)
{
    if(vValue.empty() || strKey.empty())
    {
        CCommon::HandleThrow(RF_REDIS_OP_ERR); 
    }

    vector<string> argv;
    switch(Direc)
    {
    case LEFE:
        {
            argv.push_back("LPUSH");
            break;
        }
    case RIGHT:
        {
            argv.push_back("RPUSH");
            break;
        }
    default:
        {
            CCommon::HandleThrow(RF_REDIS_OP_ERR); 
        }
    }

    argv.push_back(strKey);

    vector<string>::const_iterator it = vValue.begin();
    while(it != vValue.end())
    {
        argv.push_back(*it);
        it++;
    }

    Reply.SendSingleCmd(argv);

    //得到插入后总列表的长度
    int64_t illListLen;
    Reply.GetInteger(illListLen);

    //设置过期时间
    Reply.SetExpireSec(strKey, illSeconds);

    return 0;
}
