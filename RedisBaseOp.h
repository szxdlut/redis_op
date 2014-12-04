/*
 * =====================================================================================
 * 
 *       Filename:  RedisBaseOp.h
 * 
 *    Description:  redis基本操作的封装
 * 
 *        Version:  1.0
 *        Created:  10/30/2014 11:08:56 AM CST
 *       Revision:  none
 *       Compiler:  gcc
 * 
 *         Author:  szxdlutsun 
 *        Company:  
 * 
 * =====================================================================================
 */

#ifndef  REDISBASEOP_INC
#define  REDISBASEOP_INC

#include "HandleRedisReply.h"
#include "Common.h"

using namespace kapalai;


namespace REDIS
{

    enum emListDirec
    {
        LEFE = 1,       //左边
        RIGHT = 2,       //右边
    };



    class RedisBaseOp
    {
    public:

        static int GetHashItem(HandleReply& Reply, const string& strKey, vector<string>& vFieldValue);
        static int GetString(HandleReply& Reply, const string& strKey, string& strValue);
        static int GetHashField(HandleReply& Reply, const string& strKey, const vector<string>& vField, vector<string>& vValue);
        static int CheckKeyExists(HandleReply& Reply, const string* pstrKey, bool* pRsp);
        static int DelKeys(HandleReply& Reply, const vector<string>& vKeys);
        static int InsertHashItem(HandleReply& Reply, const string& strKey, const map<string, string>& mValue, int64_t illSeconds);
        static int RemoveFromList(HandleReply& Reply, const string& strKey, const vector<string>& vValue);
        static int BPopFromList(HandleReply& Reply, const vector<string>& vKeys, vector<string>& vValue, emListDirec Direc, int64_t illTimeOut);
        static int InsertListItem(HandleReply& Reply, const string& strKey, const vector<string>& vValue, emListDirec Direc, int64_t illSeconds);

    private:
        RedisBaseOp();
        ~RedisBaseOp() throw();
    };

}


#endif   /* ----- #ifndef REDISBASEOP_INC  ----- */

